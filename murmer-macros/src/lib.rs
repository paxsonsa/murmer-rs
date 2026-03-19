//! Murmer Proc Macros
//!
//! # `#[derive(Message)]`
//!
//! Derives [`murmer::Message`] (and optionally [`murmer::RemoteMessage`]) for a struct or enum.
//!
//! ```ignore
//! #[derive(Debug, Clone, Serialize, Deserialize, Message)]
//! #[message(result = i64)]
//! struct Increment { amount: i64 }
//! ```
//!
//! Add `remote = "..."` to also implement `RemoteMessage` with a wire-stable type ID:
//!
//! ```ignore
//! #[derive(Debug, Clone, Serialize, Deserialize, Message)]
//! #[message(result = i64, remote = "counter::Increment")]
//! struct Increment { amount: i64 }
//! ```
//!
//! # `#[handlers]` + `#[handler]`
//!
//! Place `#[handlers]` on an `impl` block containing actor message handlers.
//! Mark each handler method with `#[handler]`.
//!
//! ```ignore
//! #[handlers]
//! impl MyActor {
//!     #[handler]
//!     fn increment(&mut self, ctx: &ActorContext<Self>, state: &mut MyState, msg: Increment) -> i64 {
//!         state.count += msg.amount;
//!         state.count
//!     }
//! }
//! ```
//!
//! Generates:
//! - `impl Handler<M> for MyActor { ... }` for each `#[handler]` method
//! - `impl RemoteDispatch for MyActor { ... }` with a dispatch table matching on `TYPE_ID`

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, FnArg, ImplItem, ItemImpl, parse_macro_input};

// =============================================================================
// #[derive(Message)]
// =============================================================================

/// Derive macro for `murmer::Message` and optionally `murmer::RemoteMessage`.
///
/// # Attributes
///
/// - `#[message(result = Type)]` — **required**. The response type for this message.
/// - `#[message(result = Type, remote = "type_id")]` — also implements `RemoteMessage`
///   with the given TYPE_ID string used for wire dispatch.
///
/// # Examples
///
/// Local-only message (no serialization needed for the message itself):
/// ```ignore
/// #[derive(Debug, Message)]
/// #[message(result = ())]
/// struct Shutdown;
/// ```
///
/// Remote-capable message (requires Serialize + Deserialize):
/// ```ignore
/// #[derive(Debug, Clone, Serialize, Deserialize, Message)]
/// #[message(result = i64, remote = "counter::Increment")]
/// struct Increment { amount: i64 }
/// ```
#[proc_macro_derive(Message, attributes(message))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Parse #[message(result = Type, remote = "id")]
    let mut result_type: Option<syn::Type> = None;
    let mut remote_id: Option<String> = None;

    for attr in &input.attrs {
        if attr.path().is_ident("message") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("result") {
                    let value = meta.value()?;
                    result_type = Some(value.parse()?);
                    Ok(())
                } else if meta.path.is_ident("remote") {
                    let value = meta.value()?;
                    let lit: syn::LitStr = value.parse()?;
                    remote_id = Some(lit.value());
                    Ok(())
                } else {
                    Err(meta.error("expected `result` or `remote`"))
                }
            })
            .unwrap_or_else(|e| panic!("failed to parse #[message] attribute: {e}"));
        }
    }

    let result_ty = result_type.unwrap_or_else(|| {
        panic!("#[derive(Message)] on `{name}` requires #[message(result = Type)]")
    });

    let message_impl = quote! {
        impl #impl_generics murmer::Message for #name #ty_generics #where_clause {
            type Result = #result_ty;
        }
    };

    let remote_impl = if let Some(id) = remote_id {
        quote! {
            impl #impl_generics murmer::RemoteMessage for #name #ty_generics #where_clause {
                const TYPE_ID: &'static str = #id;
            }
        }
    } else {
        quote! {}
    };

    let output = quote! {
        #message_impl
        #remote_impl
    };

    output.into()
}

// =============================================================================
// #[handlers] + #[handler]
// =============================================================================

struct HandlerInfo {
    method_name: syn::Ident,
    msg_type: Box<syn::Type>,
}

/// Attribute macro for an impl block containing actor message handlers.
///
/// Place `#[handlers]` on a bare `impl MyActor { ... }` block. Each method
/// marked with `#[handler]` must follow this signature:
///
/// ```ignore
/// fn method_name(
///     &mut self,
///     ctx: &ActorContext<Self>,
///     state: &mut <Self as Actor>::State,
///     msg: MessageType,
/// ) -> <MessageType as Message>::Result
/// ```
///
/// The macro generates:
/// - `impl Handler<MessageType> for MyActor` for each `#[handler]` method
/// - `impl RemoteDispatch for MyActor` with a match-based dispatch table
///
/// The dispatch table is the Rust equivalent of Swift's compiler-generated
/// accessor thunks — each arm knows the concrete message type at compile time.
///
/// # Example
///
/// ```ignore
/// #[handlers]
/// impl CounterActor {
///     #[handler]
///     fn increment(&mut self, _ctx: &ActorContext<Self>, state: &mut CounterState, msg: Increment) -> i64 {
///         state.count += msg.amount;
///         state.count
///     }
///
///     #[handler]
///     fn get_count(&mut self, _ctx: &ActorContext<Self>, state: &mut CounterState, _msg: GetCount) -> i64 {
///         state.count
///     }
///
///     // Non-handler methods are preserved as-is
///     fn helper(&self) -> String { "helper".to_string() }
/// }
/// ```
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
/// Expected signature: `fn name(&mut self, ctx: &ActorContext<Self>, state: &mut State, msg: MsgType) -> Result`
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
