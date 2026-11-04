-- StateMint baseline schema (Phase 1).
-- See docs/ARCHITECTURE.md, "Data model". Configuration tables (profiles, templates, presets,
-- conditions, lookups, barcode specifications) arrive in later migrations.
-- Rules: amounts are numeric, timestamps are timestamptz (stored UTC), business dates are date.

create table client (
    id              bigserial primary key,
    code            text not null unique,
    name            text not null,
    status          text not null default 'ACTIVE' check (status in ('ACTIVE', 'INACTIVE')),
    default_language text not null default 'en',
    created_at      timestamptz not null default now()
);

create table user_account (
    id               bigserial primary key,
    username         text not null,
    password_hash    text not null,              -- Argon2id; never a plain password
    mfa_secret_enc   bytea,                      -- encrypted TOTP secret
    status           text not null default 'ACTIVE' check (status in ('ACTIVE', 'DISABLED')),
    failed_attempts  integer not null default 0,
    locked_until     timestamptz,
    last_sign_in     timestamptz,
    created_at       timestamptz not null default now()
);
create unique index user_account_username_uk on user_account (lower(username));

create table user_role (
    user_id  bigint not null references user_account (id),
    role     text not null check (role in ('VIEWER', 'OPERATOR', 'MAPPER', 'DESIGNER', 'APPROVER', 'ADMIN')),
    primary key (user_id, role)
);

create table user_client (
    user_id   bigint not null references user_account (id),
    client_id bigint not null references client (id),
    primary key (user_id, client_id)
);

create table received_file (
    id              bigserial primary key,
    client_id       bigint not null references client (id),
    source          text not null check (source in ('SFTP', 'MANUAL')),
    original_name   text not null,
    sha256          char(64) not null,
    size_bytes      bigint not null check (size_bytes > 0),
    stored_path     text not null,
    source_stamp    text,                        -- path, size and modified time at pickup
    uploaded_by     bigint references user_account (id),
    received_at     timestamptz not null default now(),
    purged_at       timestamptz,
    unique (client_id, sha256)                   -- the same content is loaded once per client
);

create table intake_event (
    id            bigserial primary key,
    client_id     bigint references client (id),
    kind          text not null check (kind in ('DUPLICATE', 'QUARANTINED', 'SAME_NAME_DIFFERENT_CONTENT')),
    original_name text not null,
    related_file  bigint references received_file (id),
    detail        text,
    created_at    timestamptz not null default now()
);

create table run (
    id                 bigserial primary key,
    file_id            bigint not null references received_file (id),
    client_id          bigint not null references client (id),
    type               text not null check (type in ('NORMAL', 'REPRINT', 'RELOAD', 'TEST')),
    parent_run_id      bigint references run (id),
    state              text not null check (state in ('RECEIVED', 'IDENTIFIED', 'PINNED', 'MAPPING', 'MAPPED',
                                                      'RENDERING', 'AWAITING_RELEASE', 'OUTPUT', 'DELIVERING',
                                                      'COMPLETE', 'HELD', 'CANCELLED')),
    held_from_state    text,
    hold_code          text,
    hold_message       text,
    accepted_checks    text[] not null default '{}',
    statement_date     date,
    run_date           date,
    profile_id         bigint,
    overrides_snapshot jsonb,
    counts             jsonb,
    control_totals     jsonb,
    virtual_time       numeric not null default 0,
    priority_weight    numeric not null default 1 check (priority_weight > 0),
    next_step_at       timestamptz,
    heartbeat_at       timestamptz,
    started_by         bigint references user_account (id),
    released_by        bigint references user_account (id),
    release_note       text,
    created_at         timestamptz not null default now(),
    completed_at       timestamptz
);
-- one normal run per file; reprints, reloads and tests may repeat
create unique index run_normal_per_file_uk on run (file_id) where type = 'NORMAL';
create index run_active_idx on run (state) where state not in ('COMPLETE', 'CANCELLED');

create table run_pin (
    run_id     bigint not null references run (id),
    kind       text not null,
    version_id bigint not null,
    primary key (run_id, kind, version_id)
);

create table run_record (
    run_id       bigint not null references run (id),
    source_line  bigint not null,
    seq          integer not null default 0,     -- several records from one XML or JSON account
    record_type  text not null,
    account_key  text not null,
    data         jsonb not null,
    primary key (run_id, source_line, seq)
);
create index run_record_account_idx on run_record (run_id, account_key);

create table run_reject (
    run_id      bigint not null references run (id),
    source_line bigint not null,
    code        text not null,
    message     text not null,
    primary key (run_id, source_line)
);

create table run_balance_issue (
    run_id      bigint not null references run (id),
    account_key text not null,
    opening     numeric not null,
    movement    numeric not null,
    closing     numeric not null,
    difference  numeric not null,
    primary key (run_id, account_key)
);

create table statement (
    id             bigserial primary key,
    run_id         bigint not null references run (id),
    account_key    text not null,
    status         text not null check (status in ('PENDING', 'IN_PROGRESS', 'RENDERED', 'FAILED', 'FILTERED')),
    filter_reason  text,
    channels       text[] not null default '{}',
    size_class     text not null default 'NORMAL' check (size_class in ('NORMAL', 'LARGE')),
    line_count     integer not null default 0,
    worker         text,
    lease_until    timestamptz,
    attempts       integer not null default 0,
    pages          integer,
    sheets         integer,
    path           text,
    sha256         char(64),
    fail_code      text,
    fail_message   text,
    unique (run_id, account_key)                 -- rendering is idempotent per account
);
create index statement_work_idx on statement (run_id, size_class, status) where status in ('PENDING', 'IN_PROGRESS');

create table output_file (
    id             bigserial primary key,
    run_id         bigint not null references run (id),
    channel        text not null,
    seq            integer not null,
    path           text not null,
    final_path     text,
    expected_pages integer not null,
    pages          integer,
    sha256         char(64),
    status         text not null check (status in ('PLANNED', 'RENAMING', 'WRITTEN')),
    unique (run_id, path)
);

create table delivery (
    id              bigserial primary key,
    output_file_id  bigint not null references output_file (id),
    destination     text not null,
    status          text not null check (status in ('PENDING', 'RETRY_WAIT', 'DELIVERED', 'FAILED')),
    attempts        integer not null default 0,
    next_attempt_at timestamptz,
    last_error      text,
    delivered_at    timestamptz,
    unique (output_file_id, destination)
);

create table outbox (
    id           bigserial primary key,
    topic        text not null,
    msg_key      text not null,
    payload      jsonb not null,
    created_at   timestamptz not null default now(),
    published_at timestamptz
);
create index outbox_unpublished_idx on outbox (id) where published_at is null;

create table audit_event (
    id          bigserial primary key,
    at          timestamptz not null default now(),
    actor       text not null,                   -- user id or SYSTEM
    action      text not null,
    entity      text not null,
    entity_id   text,
    before_hash char(64),
    after_hash  char(64),
    detail      jsonb,
    prev_hash   char(64),                        -- hash of the previous event: tampering shows
    row_hash    char(64) not null
);

create table purge_log (
    id          bigserial primary key,
    run_id      bigint references run (id),
    file_id     bigint references received_file (id),
    kinds       text not null,
    item_count  integer not null,
    bytes       bigint,
    purged_on   date not null
);
