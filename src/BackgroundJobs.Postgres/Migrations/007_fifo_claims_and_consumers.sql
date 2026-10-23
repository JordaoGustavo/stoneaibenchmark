-- FIFO claims (a share of the claims ignores priority, so low priorities are not starved) read this one.
CREATE INDEX bj_jobs_ready_fifo ON bj_jobs (queue, enqueued_at, enqueue_seq) WHERE state = 'enqueued';

-- Which node consumes which queue, renewed by every node's heartbeat; a queue with ready jobs and no live row has no
-- consumer anywhere.
CREATE TABLE bj_queue_consumers (
    queue       text        NOT NULL,
    node_id     text        NOT NULL,
    alive_until timestamptz NOT NULL,
    PRIMARY KEY (queue, node_id)
);
