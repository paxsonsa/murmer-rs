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
//! ## Explicit messages (backward compatible)
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
//! ## Auto-generated messages (new)
//!
//! ```ignore
//! #[handlers]
//! impl MyActor {
//!     #[handler]
//!     fn increment(&mut self, ctx: &ActorContext<Self>, state: &mut MyState, amount: i64) -> i64 {
//!         state.count += amount;
//!         state.count
//!     }
//!
//!     #[handler]
//!     fn get_count(&mut self, ctx: &ActorContext<Self>, state: &mut MyState) -> i64 {
//!         state.count
//!     }
//! }
//! ```
//!
//! This generates `struct Increment { pub amount: i64 }` and `struct GetCount;` with
//! all needed trait impls, plus an extension trait `MyActorExt` on `Endpoint<MyActor>`.
//!
//! ## Async handlers
//!
//! ```ignore
//! #[handlers]
//! impl MyActor {
//!     #[handler]
//!     async fn fetch(&mut self, ctx: &ActorContext<Self>, state: &mut MyState, url: String) -> String {
//!         let data = some_async_call(&url).await;
//!         data
//!     }
//! }
//! ```
//!
//! Generates `AsyncHandler<Fetch>` instead of `Handler<Fetch>`.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, FnArg, ImplItem, ItemImpl, ReturnType, parse_macro_input};

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

/// Classification of handler parameters.
enum HandlerParams {
    /// Backward compatible: last param is the message type (named msg/_msg).
    ExplicitMessage(Box<syn::Type>),
    /// Auto-generate a message struct from the remaining params.
    AutoGenParams(Vec<(syn::Ident, Box<syn::Type>)>),
}

/// Info about a single handler method.
struct HandlerInfo {
    method_name: syn::Ident,
    params: HandlerParams,
    return_type: Box<syn::Type>,
    is_async: bool,
}

/// Convert a snake_case identifier to PascalCase.
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(c) => {
                    let upper: String = c.to_uppercase().collect();
                    upper + chars.as_str()
                }
                None => String::new(),
            }
        })
        .collect()
}

/// Classify handler parameters as explicit message or auto-gen.
fn classify_handler_params(method: &syn::ImplItemFn) -> HandlerParams {
    let typed_params: Vec<_> = method
        .sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Typed(pt) => Some(pt),
            _ => None,
        })
        .collect();

    // First two typed params are ctx and state, remaining are message params
    let message_params = if typed_params.len() > 2 {
        &typed_params[2..]
    } else {
        &[]
    };

    // Single param named msg/_msg → explicit message
    if message_params.len() == 1
        && let syn::Pat::Ident(pat_ident) = &*message_params[0].pat
    {
        let name = pat_ident.ident.to_string();
        if name == "msg" || name.starts_with("_msg") {
            return HandlerParams::ExplicitMessage(message_params[0].ty.clone());
        }
    }

    // Auto-gen: extract param names and types
    let params: Vec<_> = message_params
        .iter()
        .map(|pt| {
            let name = match &*pt.pat {
                syn::Pat::Ident(pi) => pi.ident.clone(),
                _ => panic!("expected ident pattern in handler params"),
            };
            (name, pt.ty.clone())
        })
        .collect();

    HandlerParams::AutoGenParams(params)
}

/// Extract the return type from a handler method signature.
fn extract_return_type(method: &syn::ImplItemFn) -> Box<syn::Type> {
    match &method.sig.output {
        ReturnType::Default => Box::new(syn::parse_quote! { () }),
        ReturnType::Type(_, ty) => ty.clone(),
    }
}

#[proc_macro_attribute]
pub fn handlers(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let self_ty = &input.self_ty;

    // Extract the actor type name as a string for TYPE_ID generation
    let actor_type_name = quote!(#self_ty).to_string().replace(' ', "");

    let mut handler_infos: Vec<HandlerInfo> = Vec::new();
    let mut cleaned_items: Vec<ImplItem> = Vec::new();

    for item in &input.items {
        match item {
            ImplItem::Fn(method) => {
                let has_handler = method.attrs.iter().any(|a| a.path().is_ident("handler"));

                if has_handler {
                    let params = classify_handler_params(method);
                    let return_type = extract_return_type(method);
                    let is_async = method.sig.asyncness.is_some();

                    handler_infos.push(HandlerInfo {
                        method_name: method.sig.ident.clone(),
                        params,
                        return_type,
                        is_async,
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

    // ── Auto-generated message structs ──────────────────────────────

    let mut message_structs = Vec::new();
    for h in &handler_infos {
        if let HandlerParams::AutoGenParams(params) = &h.params {
            let struct_name = format_ident!("{}", to_pascal_case(&h.method_name.to_string()));
            let return_type = &h.return_type;
            let type_id = format!("{}::{}", actor_type_name, h.method_name);

            let struct_def = if params.is_empty() {
                // Unit struct
                quote! {
                    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
                    pub struct #struct_name;
                }
            } else {
                let fields = params.iter().map(|(name, ty)| {
                    quote! { pub #name: #ty }
                });
                quote! {
                    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
                    pub struct #struct_name {
                        #(#fields),*
                    }
                }
            };

            let message_impl = quote! {
                impl murmer::Message for #struct_name {
                    type Result = #return_type;
                }
                impl murmer::RemoteMessage for #struct_name {
                    const TYPE_ID: &'static str = #type_id;
                }
            };

            message_structs.push(quote! {
                #struct_def
                #message_impl
            });
        }
    }

    // ── Handler / AsyncHandler impls ────────────────────────────────

    let handler_impls: Vec<_> = handler_infos
        .iter()
        .map(|h| {
            let method_name = &h.method_name;

            // Determine the message type
            let msg_type: proc_macro2::TokenStream = match &h.params {
                HandlerParams::ExplicitMessage(ty) => quote!(#ty),
                HandlerParams::AutoGenParams(_) => {
                    let struct_name =
                        format_ident!("{}", to_pascal_case(&h.method_name.to_string()));
                    quote!(#struct_name)
                }
            };

            // Build the call expression (destructure for auto-gen)
            let call_expr: proc_macro2::TokenStream = match &h.params {
                HandlerParams::ExplicitMessage(_) => {
                    quote! { self.#method_name(ctx, state, message) }
                }
                HandlerParams::AutoGenParams(params) => {
                    if params.is_empty() {
                        quote! { self.#method_name(ctx, state) }
                    } else {
                        let field_names: Vec<_> = params.iter().map(|(name, _)| name).collect();
                        quote! { self.#method_name(ctx, state, #(message.#field_names),*) }
                    }
                }
            };

            if h.is_async {
                quote! {
                    impl murmer::AsyncHandler<#msg_type> for #self_ty {
                        fn handle<'a>(
                            &'a mut self,
                            ctx: &'a murmer::ActorContext<Self>,
                            state: &'a mut <Self as murmer::Actor>::State,
                            message: #msg_type,
                        ) -> impl ::core::future::Future<Output = <#msg_type as murmer::Message>::Result> + Send + 'a {
                            #call_expr
                        }
                    }
                }
            } else {
                quote! {
                    impl murmer::Handler<#msg_type> for #self_ty {
                        fn handle(
                            &mut self,
                            ctx: &murmer::ActorContext<Self>,
                            state: &mut <Self as murmer::Actor>::State,
                            message: #msg_type,
                        ) -> <#msg_type as murmer::Message>::Result {
                            #call_expr
                        }
                    }
                }
            }
        })
        .collect();

    // ── RemoteDispatch (async) ──────────────────────────────────────

    let match_arms = handler_infos.iter().map(|h| {
        let msg_type: proc_macro2::TokenStream = match &h.params {
            HandlerParams::ExplicitMessage(ty) => quote!(#ty),
            HandlerParams::AutoGenParams(_) => {
                let struct_name = format_ident!("{}", to_pascal_case(&h.method_name.to_string()));
                quote!(#struct_name)
            }
        };

        let handler_call = if h.is_async {
            quote! {
                <Self as murmer::AsyncHandler<#msg_type>>::handle(
                    self, ctx, state, msg,
                ).await
            }
        } else {
            quote! {
                <Self as murmer::Handler<#msg_type>>::handle(
                    self, ctx, state, msg,
                )
            }
        };

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
                let result = #handler_call;
                murmer::__reexport::bincode::serde::encode_to_vec(
                    result,
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
            async fn dispatch_remote<'a>(
                &'a mut self,
                ctx: &'a murmer::ActorContext<Self>,
                state: &'a mut <Self as murmer::Actor>::State,
                message_type: &'a str,
                payload: &'a [u8],
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

    // ── Extension trait ─────────────────────────────────────────────

    let ext_trait_name = format_ident!("{}Ext", actor_type_name);

    let ext_methods: Vec<_> = handler_infos
        .iter()
        .map(|h| {
            let method_name = &h.method_name;
            let return_type = &h.return_type;

            let msg_type: proc_macro2::TokenStream = match &h.params {
                HandlerParams::ExplicitMessage(ty) => quote!(#ty),
                HandlerParams::AutoGenParams(_) => {
                    let struct_name =
                        format_ident!("{}", to_pascal_case(&h.method_name.to_string()));
                    quote!(#struct_name)
                }
            };

            match &h.params {
                HandlerParams::ExplicitMessage(_) => {
                    // For explicit messages, the extension method takes no additional params
                    // (the user constructs the message themselves and passes it)
                    quote! {
                        fn #method_name(&self, msg: #msg_type) -> impl ::core::future::Future<Output = Result<#return_type, murmer::SendError>> + Send;
                    }
                }
                HandlerParams::AutoGenParams(params) => {
                    if params.is_empty() {
                        quote! {
                            fn #method_name(&self) -> impl ::core::future::Future<Output = Result<#return_type, murmer::SendError>> + Send;
                        }
                    } else {
                        let param_list: Vec<_> = params
                            .iter()
                            .map(|(name, ty)| quote!(#name: #ty))
                            .collect();
                        quote! {
                            fn #method_name(&self, #(#param_list),*) -> impl ::core::future::Future<Output = Result<#return_type, murmer::SendError>> + Send;
                        }
                    }
                }
            }
        })
        .collect();

    let ext_impls: Vec<_> = handler_infos
        .iter()
        .map(|h| {
            let method_name = &h.method_name;
            let return_type = &h.return_type;

            let msg_type: proc_macro2::TokenStream = match &h.params {
                HandlerParams::ExplicitMessage(ty) => quote!(#ty),
                HandlerParams::AutoGenParams(_) => {
                    let struct_name =
                        format_ident!("{}", to_pascal_case(&h.method_name.to_string()));
                    quote!(#struct_name)
                }
            };

            let send_fn = if h.is_async {
                quote!(send_async)
            } else {
                quote!(send)
            };

            match &h.params {
                HandlerParams::ExplicitMessage(_) => {
                    quote! {
                        fn #method_name(&self, msg: #msg_type) -> impl ::core::future::Future<Output = Result<#return_type, murmer::SendError>> + Send {
                            self.#send_fn(msg)
                        }
                    }
                }
                HandlerParams::AutoGenParams(params) => {
                    if params.is_empty() {
                        quote! {
                            fn #method_name(&self) -> impl ::core::future::Future<Output = Result<#return_type, murmer::SendError>> + Send {
                                self.#send_fn(#msg_type)
                            }
                        }
                    } else {
                        let param_list: Vec<_> = params
                            .iter()
                            .map(|(name, ty)| quote!(#name: #ty))
                            .collect();
                        let field_inits: Vec<_> =
                            params.iter().map(|(name, _)| quote!(#name)).collect();
                        quote! {
                            fn #method_name(&self, #(#param_list),*) -> impl ::core::future::Future<Output = Result<#return_type, murmer::SendError>> + Send {
                                self.#send_fn(#msg_type { #(#field_inits),* })
                            }
                        }
                    }
                }
            }
        })
        .collect();

    let extension_trait = quote! {
        pub trait #ext_trait_name {
            #(#ext_methods)*
        }

        impl #ext_trait_name for murmer::Endpoint<#self_ty> {
            #(#ext_impls)*
        }
    };

    // ── Auto-registration entry ──────────────────────────────────

    let auto_reg = quote! {
        const _: () = {
            #[::murmer::__reexport::linkme::distributed_slice(::murmer::ACTOR_TYPE_ENTRIES)]
            #[linkme(crate = ::murmer::__reexport::linkme)]
            static _MURMER_AUTO_REG: ::murmer::TypeRegistryEntry = ::murmer::TypeRegistryEntry {
                actor_type_name: || ::core::any::type_name::<#self_ty>(),
                register: |receptionist, label, wire_tx, response_registry, node_id, visibility| {
                    receptionist.register_remote_from_node::<#self_ty>(
                        label, wire_tx, response_registry, node_id, visibility,
                    );
                },
            };
        };
    };

    let output = quote! {
        #(#message_structs)*
        #cleaned_impl
        #(#handler_impls)*
        #remote_dispatch
        #extension_trait
        #auto_reg
    };

    output.into()
}
