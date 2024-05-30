use core::pin::pin;

use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use futures::{stream, TryStreamExt as _};
use tokio::try_join;
use tracing::info;
use wrpc_interface_blobstore::{ContainerMetadata, ObjectId, ObjectMetadata};
use wrpc_transport::{AcceptedInvocation, Transmitter as _, Value};

mod common;
use common::{init, start_nats};

#[tokio::test(flavor = "multi_thread")]
async fn rust() -> anyhow::Result<()> {
    init().await;

    let (_port, nats_client, nats_server, stop_tx) = start_nats().await?;

    let client = wrpc_transport_nats::Client::new(nats_client, "test-prefix".to_string());
    let client = Arc::new(client);

    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_clear_container()
            .await
            .context("failed to serve `clear-container`")?;
        let mut invocations = pin!(invocations);
        try_join!(
            async {
                let AcceptedInvocation {
                    params: name,
                    result_subject,
                    transmitter,
                    ..
                } = invocations
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(name, "test");
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(()))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_clear_container("test")
                    .await
                    .context("failed to invoke")?;
                let () = res.expect("invocation failed");
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;

        try_join!(
            async {
                let AcceptedInvocation {
                    params: name,
                    result_subject,
                    transmitter,
                    ..
                } = invocations
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(name, "test");
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Err::<(), _>("test"))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_clear_container("test")
                    .await
                    .context("failed to invoke")?;
                let err = res.expect_err("invocation should have failed");
                assert_eq!(err, "test");
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }

    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_container_exists()
            .await
            .context("failed to serve `container-exists`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: name,
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(name, "test");
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(true))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_container_exists("test")
                    .await
                    .context("failed to invoke")?;
                let exists = res.expect("invocation failed");
                assert!(exists);
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_create_container()
            .await
            .context("failed to serve `create-container`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: name,
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(name, "test");
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(()))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_create_container("test")
                    .await
                    .context("failed to invoke")?;
                let () = res.expect("invocation failed");
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_delete_container()
            .await
            .context("failed to serve `delete-container`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: name,
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(name, "test");
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(()))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_delete_container("test")
                    .await
                    .context("failed to invoke")?;
                let () = res.expect("invocation failed");
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_get_container_info()
            .await
            .context("failed to serve `get-container-info`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: name,
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(name, "test");
                info!("transmit response");
                transmitter
                    .transmit_static(
                        result_subject,
                        Ok::<_, String>(ContainerMetadata { created_at: 42 }),
                    )
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_get_container_info("test")
                    .await
                    .context("failed to invoke")?;
                let ContainerMetadata { created_at } = res.expect("invocation failed");
                assert_eq!(created_at, 42);
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_list_container_objects()
            .await
            .context("failed to serve `list-container-objects`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: (name, limit, offset),
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(name, "test");
                assert_eq!(limit, Some(100));
                assert_eq!(offset, None);
                info!("transmit response");
                transmitter
                    .transmit_static(
                        result_subject,
                        Ok::<_, String>(Value::Stream(Box::pin(stream::iter([Ok(vec![
                            Some("first".to_string().into()),
                            Some("second".to_string().into()),
                        ])])))),
                    )
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_list_container_objects("test", Some(100), None)
                    .await
                    .context("failed to invoke")?;
                let names = res.expect("invocation failed");
                let names = names
                    .try_collect::<Vec<_>>()
                    .await
                    .context("failed to collect names")?;
                assert_eq!(names, [["first", "second"]]);
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_copy_object()
            .await
            .context("failed to serve `copy-object`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: (src, dest),
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(
                    src,
                    ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    }
                );
                assert_eq!(
                    dest,
                    ObjectId {
                        container: "new-container".to_string(),
                        object: "new-object".to_string(),
                    }
                );
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(()))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_copy_object(
                        &ObjectId {
                            container: "container".to_string(),
                            object: "object".to_string(),
                        },
                        &ObjectId {
                            container: "new-container".to_string(),
                            object: "new-object".to_string(),
                        },
                    )
                    .await
                    .context("failed to invoke")?;
                let () = res.expect("invocation failed");
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_delete_object()
            .await
            .context("failed to serve `delete-object`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: id,
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(
                    id,
                    ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    }
                );
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(()))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_delete_object(&ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    })
                    .await
                    .context("failed to invoke")?;
                let () = res.expect("invocation failed");
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_delete_objects()
            .await
            .context("failed to serve `delete-objects`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: (container, objects),
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(container, "container".to_string());
                assert_eq!(objects, ["object".to_string(), "new-object".to_string()]);
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(()))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_delete_objects("container", ["object", "new-object"])
                    .await
                    .context("failed to invoke")?;
                let () = res.expect("invocation failed");
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_get_container_data()
            .await
            .context("failed to serve `get-container-data`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: (id, start, end),
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(
                    id,
                    ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    }
                );
                assert_eq!(start, 42);
                assert_eq!(end, 4242);
                info!("transmit response");
                transmitter
                    .transmit_static(
                        result_subject,
                        Ok::<_, String>(Value::Stream(Box::pin(stream::iter([Ok(vec![
                            Some(0x42u8.into()),
                            Some(0xffu8.into()),
                        ])])))),
                    )
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_get_container_data(
                        &ObjectId {
                            container: "container".to_string(),
                            object: "object".to_string(),
                        },
                        42,
                        4242,
                    )
                    .await
                    .context("failed to invoke")?;
                let data = res.expect("invocation failed");
                try_join!(
                    async {
                        let data = data
                            .try_collect::<Vec<_>>()
                            .await
                            .context("failed to collect data")?;
                        assert_eq!(data, [Bytes::from([0x42, 0xff].as_slice())]);
                        Ok(())
                    },
                    async {
                        info!("transmit async parameters");
                        tx.await.context("failed to transmit parameters")?;
                        info!("async parameters transmitted");
                        Ok(())
                    }
                )
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_get_object_info()
            .await
            .context("failed to serve `get-object-info`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: id,
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(
                    id,
                    ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    }
                );
                info!("transmit response");
                transmitter
                    .transmit_static(
                        result_subject,
                        Ok::<_, String>(ObjectMetadata {
                            created_at: 42,
                            size: 4242,
                        }),
                    )
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_get_object_info(&ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    })
                    .await
                    .context("failed to invoke")?;
                let v = res.expect("invocation failed");
                assert_eq!(
                    v,
                    ObjectMetadata {
                        created_at: 42,
                        size: 4242,
                    }
                );
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_has_object()
            .await
            .context("failed to serve `has-object`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: id,
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(
                    id,
                    ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    }
                );
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(true))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_has_object(&ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    })
                    .await
                    .context("failed to invoke")?;
                let v = res.expect("invocation failed");
                assert!(v);
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_move_object()
            .await
            .context("failed to serve `move-object`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: (src, dest),
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(
                    src,
                    ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    }
                );
                assert_eq!(
                    dest,
                    ObjectId {
                        container: "new-container".to_string(),
                        object: "new-object".to_string(),
                    }
                );
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(()))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_move_object(
                        &ObjectId {
                            container: "container".to_string(),
                            object: "object".to_string(),
                        },
                        &ObjectId {
                            container: "new-container".to_string(),
                            object: "new-object".to_string(),
                        },
                    )
                    .await
                    .context("failed to invoke")?;
                let () = res.expect("invocation failed");
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }
    {
        use wrpc_interface_blobstore::Blobstore;

        let invocations = client
            .serve_write_container_data()
            .await
            .context("failed to serve `move-object`")?;
        try_join!(
            async {
                let AcceptedInvocation {
                    params: (id, data),
                    result_subject,
                    transmitter,
                    ..
                } = pin!(invocations)
                    .try_next()
                    .await
                    .context("failed to receive invocation")?
                    .context("unexpected end of stream")?;
                assert_eq!(
                    id,
                    ObjectId {
                        container: "container".to_string(),
                        object: "object".to_string(),
                    }
                );
                let data = data
                    .map_ok(|buf| String::from_utf8(buf.to_vec()).unwrap())
                    .try_collect::<Vec<_>>()
                    .await
                    .context("failed to collect data")?;
                assert_eq!(data, ["test"]);
                info!("transmit response");
                transmitter
                    .transmit_static(result_subject, Ok::<_, String>(()))
                    .await
                    .context("failed to transmit response")?;
                info!("response transmitted");
                anyhow::Ok(())
            },
            async {
                info!("invoke function");
                let (res, tx) = client
                    .invoke_write_container_data(
                        &ObjectId {
                            container: "container".to_string(),
                            object: "object".to_string(),
                        },
                        Box::pin(stream::iter(["test".into()])),
                    )
                    .await
                    .context("failed to invoke")?;
                let () = res.expect("invocation failed");
                info!("transmit async parameters");
                tx.await.context("failed to transmit parameters")?;
                info!("async parameters transmitted");
                Ok(())
            }
        )?;
    }

    stop_tx.send(()).expect("failed to stop NATS.io server");
    nats_server
        .await
        .context("failed to await NATS.io server stop")?
        .context("NATS.io server failed to stop")?;
    Ok(())
}
