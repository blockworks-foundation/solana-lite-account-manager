use {
    crate::snapshot_utils::{AppendVec, AppendVecIterator},
    tokio::task::JoinSet,
};

#[async_trait::async_trait]
pub trait AppendVecConsumer {
    async fn on_append_vec(&mut self, append_vec: AppendVec) -> anyhow::Result<()>;
}

pub async fn par_iter_append_vecs<F, A>(
    iterator: AppendVecIterator<'_>,
    create_consumer: F,
    num_threads: usize,
) -> anyhow::Result<()>
where
    F: Fn() -> A,
    A: AppendVecConsumer + Send + 'static,
{
    let mut tasks = JoinSet::new();
    for append_vec in iterator {
        let mut consumer = if tasks.len() >= num_threads {
            tasks.join_next().await.expect("checked")??
        } else {
            create_consumer()
        };

        tasks.spawn(async move {
            consumer.on_append_vec(append_vec?).await?;
            Ok::<_, anyhow::Error>(consumer)
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}
