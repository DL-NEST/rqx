use proc_macro::TokenStream;

use quote::quote;
use syn::parse_macro_input;

#[proc_macro_attribute]
pub fn r_main(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as syn::ItemFn);
    let sig = &mut input.sig;
    let vis = &input.vis;
    let body = &input.block;
    let attrs = &input.attrs;
    let name = &sig.ident;

    if sig.ident == "main" && !sig.inputs.is_empty() {
        return syn::Error::new_spanned(&sig.ident, "the main function cannot accept arguments")
            .to_compile_error()
            .into();
    }

    if sig.asyncness.is_none() {
        return syn::Error::new_spanned(sig.fn_token, "only async fn is supported")
            .to_compile_error()
            .into();
    }

    sig.asyncness = None;

    quote! {
        #(#attrs)*
        #vis #sig {
            println!(stringify!(#name));
            #body
        }
    }
    .into()
}

#[proc_macro_attribute]
pub fn r_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
