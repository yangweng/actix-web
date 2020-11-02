use std::marker::PhantomData;
use std::task::{Context, Poll};

use actix_codec::Framed;
use actix_service::{Service, ServiceFactory};
use futures_util::future::Ready;

use crate::error::Error;
use crate::h1::Codec;
use crate::request::Request;
use actix_server::ServiceStream;

pub struct UpgradeHandler<T>(PhantomData<T>);

impl<T: ServiceStream> ServiceFactory for UpgradeHandler<T> {
    type Request = (Request, Framed<T, Codec<T::Runtime>>);
    type Response = ();
    type Error = Error;
    type Config = ();
    type Service = UpgradeHandler<T>;
    type InitError = Error;
    type Future = Ready<Result<Self::Service, Self::InitError>>;

    fn new_service(&self, _: ()) -> Self::Future {
        unimplemented!()
    }
}

impl<T: ServiceStream> Service for UpgradeHandler<T> {
    type Request = (Request, Framed<T, Codec<T::Runtime>>);
    type Response = ();
    type Error = Error;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Self::Request) -> Self::Future {
        unimplemented!()
    }
}
