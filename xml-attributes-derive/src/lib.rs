//#![feature(trace_macros)]
extern crate proc_macro2;
extern crate proc_macro;
#[macro_use]
extern crate quote;
extern crate syn;

use quote::ToTokens;
use proc_macro::TokenStream;
use proc_macro2::{Literal, Spacing, Span, TokenNode, TokenTree};
use syn::{Data, Ident, Type};

#[proc_macro_derive(FromXmlAttributes)]
pub fn from_xml_attributes(input: TokenStream) -> TokenStream {
    // Parse the input stream
    let ast = syn::parse(input).unwrap();

    // Build the impl
    let gen = impl_xml(&ast);

    // Return the generated impl
    gen.into()
}

fn impl_xml(ast: &syn::DeriveInput) -> quote::Tokens {
    let name = &ast.ident;
    match ast.data {
        Data::Struct(ref data) => impl_struct_xml_fields(name, &data.fields),
        Data::Enum(ref data) => enum_fields(name, data),
        Data::Union(ref data) => union_fields(name, data),
    }
}

fn union_fields(_name: &syn::Ident, _data: &syn::DataUnion) -> quote::Tokens {
    let mut result = Vec::new();
    result.push(quote!{
        panic!("not implemented");
    });
    quote! {
        #(#result)*
    }
}

fn enum_fields(_name: &syn::Ident, _variants: &syn::DataEnum) -> quote::Tokens {
    let mut result = Vec::new();
    result.push(quote!{
        panic!("not implemented");
    });
    quote! {
        #(#result)*
    }
}

fn impl_struct_xml_fields(name: &syn::Ident, fields: &syn::Fields) -> quote::Tokens {
    let mut result = Vec::new();
    for field in fields.iter() {
        let ident = &field.ident;
        let ident_type = match field.clone().ty {
            Type::Path(p) => if let Some(i) = p.path.segments.clone().into_iter().next() {
                Some(i.ident)
            } else {
                None
            },
            _ => None,
        };
        let u_64 = Ident::new("u64", Span::def_site());
        let f_64 = Ident::new("f64", Span::def_site());
        let string = Ident::new("String", Span::def_site());

        // Setup the fields
        match ident_type {
            Some(i_type) => {
                if i_type == u_64 {
                    result.push(quote!{
                        let mut #ident = 0;
                    });
                } else if i_type == f_64 {
                    result.push(quote!{
                        let mut #ident = 0.0;
                    });
                } else if i_type == string {
                    result.push(quote!{
                        let mut #ident = String::new();
                    });
                } else {
                    // Uncomment me to debug why some fields may be missing
                    //println!("else: {:?} {:?} {:?}", ident, i_type, field.clone().ty);
                }
            }
            None => {
                // Unable to identify this type
                println!("Unable to identify type for {:?}", ident);
            }
        }
    }

    // Match the fields
    result.push(quote!{
        for a in attrs
    });

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('{', Spacing::Joint),
        }.into_tokens(),
    );

    result.push(quote!{
        let item = a?;
        let val = String::from_utf8_lossy(&item.value);
        match item.key
    });

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('{', Spacing::Joint),
        }.into_tokens(),
    );

    for field in fields.iter() {
        let ident = &field.ident;
        let ident_type = match field.clone().ty {
            Type::Path(p) => if let Some(i) = p.path.segments.clone().into_iter().next() {
                Some(i.ident)
            } else {
                None
            },
            _ => None,
        };
        let u_64 = Ident::new("u64", Span::def_site());
        let f_64 = Ident::new("f64", Span::def_site());
        let string = Ident::new("String", Span::def_site());
        let i = ident.unwrap();
        let ident_name = {
            let i = i.as_ref();
            if i.starts_with("_") {
                i.trim_left_matches("_")
            } else {
                i
            }
        };

        match ident_type {
            Some(i_type) => {
                if i_type == u_64 {
                    result.push(
                        TokenTree {
                            span: Span::def_site(),
                            kind: TokenNode::Literal(Literal::byte_string(ident_name.as_bytes())),
                        }.into_tokens(),
                    );

                    result.push(quote!{
                        => {
                            #ident = u64::from_str(&val)?;
                        }
                    });
                } else if i_type == f_64 {
                    result.push(
                        TokenTree {
                            span: Span::def_site(),
                            kind: TokenNode::Literal(Literal::byte_string(ident_name.as_bytes())),
                        }.into_tokens(),
                    );

                    result.push(quote!{
                        => {
                            #ident = f64::from_str(&val)?;
                        }
                    });
                } else if i_type == string {
                    result.push(
                        TokenTree {
                            span: Span::def_site(),
                            kind: TokenNode::Literal(Literal::byte_string(ident_name.as_bytes())),
                        }.into_tokens(),
                    );

                    result.push(quote!{
                        => {
                            #ident = val.to_string();
                        }
                    });
                } else {
                    // Uncomment me to debug why some fields may be missing
                    //println!("else: {:?} {:?}", ident, i_type);
                }
            }
            None => {
                // Unable to identify this type
                println!("Unable to identify type for {:?}", ident);
            }
        }
    }

    result.push(quote!{
        _ => {
            debug!(
                "unknown xml attribute: {}",
                String::from_utf8_lossy(item.key)
            );
        }
    });

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('}', Spacing::Joint),
        }.into_tokens(),
    );
    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('}', Spacing::Joint),
        }.into_tokens(),
    );
    result.push(quote!{
        Ok
    });

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('(', Spacing::Joint),
        }.into_tokens(),
    );

    result.push(quote!{
        #name
    });

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('{', Spacing::Joint),
        }.into_tokens(),
    );

    for field in fields.iter() {
        let ident = &field.ident;

        result.push(quote!{
            #ident: #ident,
        });
    }

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('}', Spacing::Joint),
        }.into_tokens(),
    );
    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op(')', Spacing::Joint),
        }.into_tokens(),
    );

    quote! {
        impl FromXmlAttributes for #name {
            fn from_xml_attributes(attrs: Attributes) -> MetricsResult<Self> {
                #(#result)*
            }
        }
    }
}
