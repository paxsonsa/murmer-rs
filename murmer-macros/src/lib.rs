//! Murmer Proc Macros
//!
//! `#[handlers]` on an impl block:
//!   - Finds methods marked with `#[handler]`
//!   - Generates `Handler<M>` trait impls for each
//!   - Generates `RemoteDispatch` impl with dispatch table
//!
//! The generated dispatch table is the Rust equivalent of Swift's
//! compiler-generated accessor thunks — each arm knows the concrete
//! message type at compile time.

use proc_macro::TokenStream;
use quote::quote;
use syn::{FnArg, ImplItem, ItemImpl, parse_macro_input};

struct HandlerInfo {
    method_name: syn::Ident,
    msg_type: Box<syn::Type>,
}

/// Attribute macro for an impl block containing actor message handlers.
///
/// Usage:
/// ```ignore
/// #[handlers]
/// impl MyActor {
///     #[handler]
///     fn do_something(&mut self, state: &mut MyState, msg: DoSomething) -> Result {
///         // ...
///     }
/// }
/// ```
///
/// Generates:
/// - `impl Handler<DoSomething> for MyActor { ... }` for each `#[handler]` method
/// - `impl RemoteDispatch for MyActor { ... }` with a match arm per handler
#[proc_macro_attribute]
pub fn handlers(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let self_ty = &input.self_ty;

    let mut handler_infos: Vec<HandlerInfo> = Vec::new();
    let mut cleaned_items: Vec<ImplItem> = Vec::new();

    for item in &input.items {
        match item {
            ImplItem::Fn(method) => {
                let has_handler = method.attrs.iter().any(|a| a.path().is_ident("handler"));

                if has_handler {
                    // Extract message type from the last parameter
                    let msg_type = extract_message_type(method);
                    handler_infos.push(HandlerInfo {
                        method_name: method.sig.ident.clone(),
                        msg_type,
                    });

                    // Keep the method but strip the #[handler] attribute
                    let mut cleaned = method.clone();
                    cleaned.attrs.retain(|a| !a.path().is_ident("handler"));
                    cleaned_items.push(ImplItem::Fn(cleaned));
                } else {
                    cleaned_items.push(item.clone());
                }
            }
            other => cleaned_items.push(other.clone()),
        }
    }

    // Rebuild the impl block without #[handler] attributes
    let mut cleaned_impl = input.clone();
    cleaned_impl.items = cleaned_items;

    // Generate Handler<M> impls — each delegates to the named method
    let handler_impls = handler_infos.iter().map(|h| {
        let method_name = &h.method_name;
        let msg_type = &h.msg_type;
        quote! {
            impl murmer::Handler<#msg_type> for #self_ty {
                fn handle(
                    &mut self,
                    ctx: &murmer::ActorContext<Self>,
                    state: &mut <Self as murmer::Actor>::State,
                    message: #msg_type,
                ) -> <#msg_type as murmer::Message>::Result {
                    self.#method_name(ctx, state, message)
                }
            }
        }
    });

    // Generate RemoteDispatch — the dispatch table
    let match_arms = handler_infos.iter().map(|h| {
        let msg_type = &h.msg_type;
        quote! {
            <#msg_type as murmer::RemoteMessage>::TYPE_ID => {
                let (msg, _): (#msg_type, _) =
                    murmer::__reexport::bincode::serde::decode_from_slice(
                        payload,
                        murmer::__reexport::bincode::config::standard(),
                    )
                    .map_err(|e| murmer::DispatchError::DeserializeFailed(
                        e.to_string(),
                    ))?;
                let result = <Self as murmer::Handler<#msg_type>>::handle(
                    self, ctx, state, msg,
                );
                murmer::__reexport::bincode::serde::encode_to_vec(
                    &result,
                    murmer::__reexport::bincode::config::standard(),
                )
                .map_err(|e| murmer::DispatchError::SerializeFailed(
                    e.to_string(),
                ))
            }
        }
    });

    let remote_dispatch = quote! {
        impl murmer::RemoteDispatch for #self_ty {
            fn dispatch_remote(
                &mut self,
                ctx: &murmer::ActorContext<Self>,
                state: &mut <Self as murmer::Actor>::State,
                message_type: &str,
                payload: &[u8],
            ) -> Result<Vec<u8>, murmer::DispatchError> {
                match message_type {
                    #(#match_arms)*
                    __other => Err(murmer::DispatchError::UnknownMessageType(
                        __other.to_string(),
                    )),
                }
            }
        }
    };

    let output = quote! {
        #cleaned_impl
        #(#handler_impls)*
        #remote_dispatch
    };

    output.into()
}

/// Extract the message type from a handler method's last parameter.
///
/// Expected signature: `fn name(&mut self, state: &mut State, msg: MsgType) -> Result`
fn extract_message_type(method: &syn::ImplItemFn) -> Box<syn::Type> {
    let inputs: Vec<_> = method.sig.inputs.iter().collect();

    // Find the last typed (non-self) argument — that's the message
    let last_arg = inputs
        .iter()
        .rev()
        .find(|arg| matches!(arg, FnArg::Typed(_)))
        .unwrap_or_else(|| {
            panic!(
                "handler method `{}` must have a message parameter",
                method.sig.ident
            )
        });

    match last_arg {
        FnArg::Typed(pat_type) => pat_type.ty.clone(),
        _ => unreachable!(),
    }
}
