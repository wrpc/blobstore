use core::future::Future;
use core::pin::Pin;
use core::time::Duration;

use std::sync::Arc;

use anyhow::{bail, Context};
use bytes::Bytes;
use futures::stream::select_all;
use futures::{stream, Stream, StreamExt as _, TryStreamExt as _};
use tokio::select;
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::info;
use wrpc_interface_blobstore::bindings::wrpc::blobstore::types::{
    ContainerMetadata, ObjectId, ObjectMetadata,
};

mod common;
use common::start_nats;

#[derive(Clone)]
struct Handler;

impl<Ctx: Send>
    wrpc_interface_blobstore::bindings::exports::wrpc::blobstore::blobstore::Handler<Ctx>
    for Handler
{
    async fn clear_container(&self, _cx: Ctx, name: String) -> anyhow::Result<Result<(), String>> {
        assert_eq!(name, "test");
        Ok(Ok(()))
    }

    async fn container_exists(
        &self,
        _cx: Ctx,
        name: String,
    ) -> anyhow::Result<Result<bool, String>> {
        assert_eq!(name, "test");
        Ok(Ok(true))
    }

    async fn create_container(&self, _cx: Ctx, name: String) -> anyhow::Result<Result<(), String>> {
        assert_eq!(name, "test");
        Ok(Ok(()))
    }

    async fn delete_container(&self, _cx: Ctx, name: String) -> anyhow::Result<Result<(), String>> {
        assert_eq!(name, "test");
        Ok(Ok(()))
    }

    async fn get_container_info(
        &self,
        _cx: Ctx,
        name: String,
    ) -> anyhow::Result<Result<ContainerMetadata, String>> {
        assert_eq!(name, "test");
        Ok(Ok(ContainerMetadata { created_at: 42 }))
    }

    async fn list_container_objects(
        &self,
        _cx: Ctx,
        name: String,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> anyhow::Result<
        Result<
            (
                Pin<Box<dyn Stream<Item = Vec<String>> + Send>>,
                Pin<Box<dyn Future<Output = Result<(), String>> + Send>>,
            ),
            String,
        >,
    > {
        assert_eq!(name, "test");
        assert_eq!(limit, Some(100));
        assert_eq!(offset, None);
        Ok(Ok((
            Box::pin(stream::iter([
                vec!["first".to_string()],
                vec!["second".to_string()],
            ])),
            Box::pin(async { Ok(()) }),
        )))
    }

    async fn copy_object(
        &self,
        _cx: Ctx,
        src: ObjectId,
        dest: ObjectId,
    ) -> anyhow::Result<Result<(), String>> {
        assert_eq!(src.container, "container");
        assert_eq!(src.object, "object");
        assert_eq!(dest.container, "new-container");
        assert_eq!(dest.object, "new-object");
        Ok(Ok(()))
    }

    async fn delete_object(&self, _cx: Ctx, id: ObjectId) -> anyhow::Result<Result<(), String>> {
        assert_eq!(id.container, "container");
        assert_eq!(id.object, "object");
        Ok(Ok(()))
    }

    async fn delete_objects(
        &self,
        _cx: Ctx,
        container: String,
        objects: Vec<String>,
    ) -> anyhow::Result<Result<(), String>> {
        assert_eq!(container, "container".to_string());
        assert_eq!(objects, ["object".to_string(), "new-object".to_string()]);
        Ok(Ok(()))
    }

    async fn get_container_data(
        &self,
        _cx: Ctx,
        id: ObjectId,
        start: u64,
        end: u64,
    ) -> anyhow::Result<
        Result<
            (
                Pin<Box<dyn Stream<Item = Bytes> + Send>>,
                Pin<Box<dyn Future<Output = Result<(), String>> + Send>>,
            ),
            String,
        >,
    > {
        assert_eq!(id.container, "container");
        assert_eq!(id.object, "object");
        assert_eq!(start, 42);
        assert_eq!(end, 4242);
        Ok(Ok((
            Box::pin(stream::iter([Bytes::from("foo"), Bytes::from("bar")])),
            Box::pin(async { Ok(()) }),
        )))
    }

    async fn get_object_info(
        &self,
        _cx: Ctx,
        id: ObjectId,
    ) -> anyhow::Result<Result<ObjectMetadata, String>> {
        assert_eq!(id.container, "container");
        assert_eq!(id.object, "object");
        Ok(Ok(ObjectMetadata {
            created_at: 42,
            size: 4242,
        }))
    }

    async fn has_object(&self, _cx: Ctx, id: ObjectId) -> anyhow::Result<Result<bool, String>> {
        assert_eq!(id.container, "container");
        assert_eq!(id.object, "object");
        Ok(Ok(true))
    }

    async fn move_object(
        &self,
        _cx: Ctx,
        src: ObjectId,
        dest: ObjectId,
    ) -> anyhow::Result<Result<(), String>> {
        assert_eq!(src.container, "container");
        assert_eq!(src.object, "object");
        assert_eq!(dest.container, "new-container");
        assert_eq!(dest.object, "new-object");
        Ok(Ok(()))
    }

    async fn write_container_data(
        &self,
        _cx: Ctx,
        id: ObjectId,
        data: Pin<Box<dyn Stream<Item = Bytes> + Send>>,
    ) -> anyhow::Result<Result<Pin<Box<dyn Future<Output = Result<(), String>> + Send>>, String>>
    {
        assert_eq!(id.container, "container");
        assert_eq!(id.object, "object");
        Ok(Ok(Box::pin(async move {
            assert_eq!(data.collect::<Vec<Bytes>>().await.concat(), b"foobar");
            Ok(())
        })))
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn rust() -> anyhow::Result<()> {
    let (_port, nats_client, nats_server, stop_tx) = start_nats().await?;

    let client = wrpc_transport_nats::Client::new(nats_client, "test-prefix".to_string(), None);
    let client = Arc::new(client);

    let shutdown = Arc::new(Notify::new());

    info!("serving blobstore");
    let server = tokio::spawn({
        let client = Arc::clone(&client);
        let shutdown = Arc::clone(&shutdown);
        async move {
            let invocations = wrpc_interface_blobstore::bindings::exports::wrpc::blobstore::blobstore::serve_interface(
                client.as_ref(),
                Handler,
            )
            .await
            .context("failed to serve blobstore")?;
            let mut invocations = select_all(invocations.into_iter().map(
                |(instance, name, invocations)| {
                    invocations
                        .try_buffer_unordered(16) // handle up to 16 invocations concurrently
                        .map(move |res| (instance, name, res))
                },
            ));
            loop {
                select! {
                    Some((instance, name, res)) = invocations.next() => {
                        match res {
                            Ok(()) => {
                                info!(instance, name, "invocation successfully handled");
                            }
                            Err(err) => {
                                bail!(err.context("failed to accept invocation"))
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        return Ok(())
                    }
                }
            }
        }
    });

    // wait for the server to start
    sleep(Duration::from_millis(300)).await;

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::clear_container(
        client.as_ref(),
        None,
        "test",
    )
    .await?;
    assert_eq!(res, Ok(()));

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::container_exists(
        client.as_ref(),
        None,
        "test",
    )
    .await?;
    assert_eq!(res, Ok(true));

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::create_container(
        client.as_ref(),
        None,
        "test",
    )
    .await?;
    assert_eq!(res, Ok(()));

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::delete_container(
        client.as_ref(),
        None,
        "test",
    )
    .await?;
    assert_eq!(res, Ok(()));

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::get_container_info(
        client.as_ref(),
        None,
        "test",
    )
    .await?;
    assert!(matches!(res, Ok(ContainerMetadata { created_at: 42 })));

    let (res, io) =
        wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::list_container_objects(
            client.as_ref(),
            None,
            "test",
            Some(100),
            None,
        )
        .await?;
    if let Some(io) = io {
        io.await.expect("failed to complete async I/O");
    }
    let (stream, fut) = res.unwrap();
    assert_eq!(
        stream.collect::<Vec<_>>().await.concat(),
        ["first", "second"]
    );
    fut.await.expect("future failed");

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::copy_object(
        client.as_ref(),
        None,
        &ObjectId {
            container: "container".to_string(),
            object: "object".to_string(),
        },
        &ObjectId {
            container: "new-container".to_string(),
            object: "new-object".to_string(),
        },
    )
    .await?;
    assert_eq!(res, Ok(()));

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::delete_object(
        client.as_ref(),
        None,
        &ObjectId {
            container: "container".to_string(),
            object: "object".to_string(),
        },
    )
    .await?;
    assert_eq!(res, Ok(()));

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::delete_objects(
        client.as_ref(),
        None,
        "container",
        &["object", "new-object"],
    )
    .await?;
    assert_eq!(res, Ok(()));

    let (res, io) =
        wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::get_container_data(
            client.as_ref(),
            None,
            &ObjectId {
                container: "container".to_string(),
                object: "object".to_string(),
            },
            42,
            4242,
        )
        .await?;
    if let Some(io) = io {
        io.await.expect("failed to complete async I/O");
    }
    let (stream, fut) = res.unwrap();
    assert_eq!(stream.collect::<Vec<_>>().await.concat(), b"foobar");
    fut.await.expect("future failed");

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::get_object_info(
        client.as_ref(),
        None,
        &ObjectId {
            container: "container".to_string(),
            object: "object".to_string(),
        },
    )
    .await?;
    assert!(matches!(
        res,
        Ok(ObjectMetadata {
            created_at: 42,
            size: 4242
        })
    ));

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::has_object(
        client.as_ref(),
        None,
        &ObjectId {
            container: "container".to_string(),
            object: "object".to_string(),
        },
    )
    .await?;
    assert_eq!(res, Ok(true));

    let res = wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::move_object(
        client.as_ref(),
        None,
        &ObjectId {
            container: "container".to_string(),
            object: "object".to_string(),
        },
        &ObjectId {
            container: "new-container".to_string(),
            object: "new-object".to_string(),
        },
    )
    .await?;
    assert_eq!(res, Ok(()));

    let (res, io) =
        wrpc_interface_blobstore::bindings::wrpc::blobstore::blobstore::write_container_data(
            client.as_ref(),
            None,
            &ObjectId {
                container: "container".to_string(),
                object: "object".to_string(),
            },
            Box::pin(stream::iter([Bytes::from("foo"), Bytes::from("bar")])),
        )
        .await?;
    if let Some(io) = io {
        io.await.expect("failed to complete async I/O");
    }
    let fut = res.unwrap();
    fut.await.expect("future failed");

    shutdown.notify_one();
    server.await??;

    stop_tx.send(()).expect("failed to stop NATS.io server");
    nats_server
        .await
        .context("failed to await NATS.io server stop")?
        .context("NATS.io server failed to stop")?;
    Ok(())
}
