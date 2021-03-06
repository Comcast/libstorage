extern crate proc_macro;
/**
* Copyright 2019 Comcast Cable Communications Management, LLC
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* SPDX-License-Identifier: Apache-2.0
*/
extern crate proc_macro2;
#[macro_use]
extern crate quote;
extern crate syn;

use proc_macro::TokenStream;
use proc_macro2::{Literal, Spacing, Span, TokenNode, TokenTree};
use quote::ToTokens;
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
    result.push(quote! {
        panic!("not implemented");
    });
    quote! {
        #(#result)*
    }
}

fn enum_fields(_name: &syn::Ident, _variants: &syn::DataEnum) -> quote::Tokens {
    let mut result = Vec::new();
    result.push(quote! {
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
            Type::Path(p) => {
                if let Some(i) = p.path.segments.clone().into_iter().next() {
                    Some(i.ident)
                } else {
                    None
                }
            }
            _ => None,
        };
        let u_64 = Ident::new("u64", Span::def_site());
        let f_64 = Ident::new("f64", Span::def_site());
        let string = Ident::new("String", Span::def_site());
        let boolean = Ident::new("bool", Span::def_site());

        // Setup the fields
        match ident_type {
            Some(i_type) => {
                if i_type == u_64 {
                    result.push(quote! {
                        let mut #ident = 0;
                    });
                } else if i_type == f_64 {
                    result.push(quote! {
                        let mut #ident = 0.0;
                    });
                } else if i_type == string {
                    result.push(quote! {
                        let mut #ident = String::new();
                    });
                } else if i_type == boolean {
                    result.push(quote! {
                        let mut #ident = false;
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
    result.push(quote! {
        for a in attrs
    });

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('{', Spacing::Joint),
        }
        .into_tokens(),
    );

    result.push(quote! {
        let item = a?;
        let val = String::from_utf8_lossy(&item.value);
        match item.key
    });

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('{', Spacing::Joint),
        }
        .into_tokens(),
    );

    for field in fields.iter() {
        let ident = &field.ident;
        let ident_type = match field.clone().ty {
            Type::Path(p) => {
                if let Some(i) = p.path.segments.clone().into_iter().next() {
                    Some(i.ident)
                } else {
                    None
                }
            }
            _ => None,
        };
        let u_64 = Ident::new("u64", Span::def_site());
        let f_64 = Ident::new("f64", Span::def_site());
        let string = Ident::new("String", Span::def_site());
        let boolean = Ident::new("bool", Span::def_site());

        let i = ident.unwrap();
        let ident_name = {
            let i = i.as_ref();
            if i.starts_with("_") {
                i.trim_start_matches("_")
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
                        }
                        .into_tokens(),
                    );

                    result.push(quote! {
                        => {
                            #ident = u64::from_str(&val)?;
                        }
                    });
                } else if i_type == f_64 {
                    result.push(
                        TokenTree {
                            span: Span::def_site(),
                            kind: TokenNode::Literal(Literal::byte_string(ident_name.as_bytes())),
                        }
                        .into_tokens(),
                    );

                    result.push(quote! {
                        => {
                            #ident = f64::from_str(&val)?;
                        }
                    });
                } else if i_type == string {
                    result.push(
                        TokenTree {
                            span: Span::def_site(),
                            kind: TokenNode::Literal(Literal::byte_string(ident_name.as_bytes())),
                        }
                        .into_tokens(),
                    );

                    result.push(quote! {
                        => {
                            #ident = val.to_string();
                        }
                    });
                } else if i_type == boolean {
                    result.push(
                        TokenTree {
                            span: Span::def_site(),
                            kind: TokenNode::Literal(Literal::byte_string(ident_name.as_bytes())),
                        }
                        .into_tokens(),
                    );

                    result.push(quote! {
                        => {
                            #ident = bool::from_str(&val)?;
                        }
                    });
                } else {
                    // Uncomment me to debug why some fields may be missing
                    println!("else: {:?} {:?}", ident, i_type);
                }
            }
            None => {
                // Unable to identify this type
                println!("Unable to identify type for {:?}", ident);
            }
        }
    }

    result.push(quote! {
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
        }
        .into_tokens(),
    );
    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('}', Spacing::Joint),
        }
        .into_tokens(),
    );
    result.push(quote! {
        Ok
    });

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('(', Spacing::Joint),
        }
        .into_tokens(),
    );

    result.push(quote! {
        #name
    });

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('{', Spacing::Joint),
        }
        .into_tokens(),
    );

    for field in fields.iter() {
        let ident = &field.ident;

        result.push(quote! {
            #ident: #ident,
        });
    }

    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op('}', Spacing::Joint),
        }
        .into_tokens(),
    );
    result.push(
        TokenTree {
            span: Span::def_site(),
            kind: TokenNode::Op(')', Spacing::Joint),
        }
        .into_tokens(),
    );

    quote! {
        impl FromXmlAttributes for #name {
            fn from_xml_attributes(attrs: Attributes) -> MetricsResult<Self> {
                #(#result)*
            }
        }
    }
}
