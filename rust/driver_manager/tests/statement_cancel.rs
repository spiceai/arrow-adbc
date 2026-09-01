// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `AdbcStatementCancel` has to reach a statement call that is already running.
//!
//! It is the one statement function the specification allows to be called while
//! another statement function is in flight, and interrupting a long remote query
//! is the only thing it is for. A driver manager that serializes it behind the
//! call it is meant to interrupt turns it into a no-op that returns once the
//! query has finished on its own.
//!
//! The driver here is built in the test rather than loaded: `StatementExecuteQuery`
//! blocks until `StatementCancel` releases it, so the test can only pass if the
//! cancel is delivered concurrently.

use std::ffi::{c_char, c_int, c_void};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use adbc_core::constants::{ADBC_STATUS_CANCELLED, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_OK};
use adbc_core::error::{AdbcStatusCode, Status};
use adbc_core::options::AdbcVersion;
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_manager::ManagedDriver;

/// How long `StatementExecuteQuery` waits for a cancel before giving up.
///
/// Generous, because it is the failure timeout: with the cancel serialized
/// behind the execute, nothing arrives and the test has to end somehow.
const EXECUTE_GIVE_UP: Duration = Duration::from_secs(30);

/// Serializes the tests below.
///
/// They share the driver state under the C-ABI functions, and the tests in one
/// integration binary run concurrently: without this, one test's `reset_state`
/// erases the other's cancellation, which shows up as a flake or a 30-second
/// wait rather than as a failure.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// The one query in flight. A single statement per test is enough, and a static
/// keeps the C-ABI functions free of per-statement state.
static CANCELLED: Mutex<bool> = Mutex::new(false);
static CANCEL_SIGNAL: Condvar = Condvar::new();
/// Set while `StatementExecuteQuery` is blocked, so the test cancels a call that
/// is genuinely in flight rather than one that has not started.
static EXECUTING: AtomicBool = AtomicBool::new(false);
/// How many times the driver has been asked to release the statement. One
/// statement must be released once, however many handles the caller held.
static RELEASES: AtomicUsize = AtomicUsize::new(0);

fn reset_state() {
    *CANCELLED
        .lock()
        .expect("the cancel state should be lockable") = false;
    EXECUTING.store(false, Ordering::SeqCst);
    RELEASES.store(0, Ordering::SeqCst);
}

unsafe extern "C" fn noop_database(
    _database: *mut adbc_ffi::FFI_AdbcDatabase,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_new(
    _connection: *mut adbc_ffi::FFI_AdbcConnection,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_init(
    _connection: *mut adbc_ffi::FFI_AdbcConnection,
    _database: *mut adbc_ffi::FFI_AdbcDatabase,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_release(
    _connection: *mut adbc_ffi::FFI_AdbcConnection,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_new(
    _connection: *mut adbc_ffi::FFI_AdbcConnection,
    statement: *mut adbc_ffi::FFI_AdbcStatement,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    // Non-null private data is what marks a statement as allocated.
    unsafe { (*statement).private_data = 1 as *mut c_void };
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_release(
    statement: *mut adbc_ffi::FFI_AdbcStatement,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    RELEASES.fetch_add(1, Ordering::SeqCst);
    unsafe { (*statement).private_data = null_mut() };
    ADBC_STATUS_OK
}

/// Refuses a released statement, as a real driver does.
unsafe extern "C" fn statement_set_sql_query(
    statement: *mut adbc_ffi::FFI_AdbcStatement,
    _query: *const c_char,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    if unsafe { (*statement).private_data }.is_null() {
        return ADBC_STATUS_INVALID_STATE;
    }
    ADBC_STATUS_OK
}

/// Blocks like a driver waiting on a remote query, and returns only when
/// cancelled.
unsafe extern "C" fn statement_execute_query(
    _statement: *mut adbc_ffi::FFI_AdbcStatement,
    _stream: *mut arrow_array::ffi_stream::FFI_ArrowArrayStream,
    _rows_affected: *mut i64,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    EXECUTING.store(true, Ordering::SeqCst);
    let mut cancelled = CANCELLED
        .lock()
        .expect("the cancel state should be lockable");
    let deadline = Instant::now() + EXECUTE_GIVE_UP;
    while !*cancelled {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            EXECUTING.store(false, Ordering::SeqCst);
            return ADBC_STATUS_OK;
        }
        let (guard, _) = CANCEL_SIGNAL
            .wait_timeout(cancelled, remaining)
            .expect("the cancel state should be lockable");
        cancelled = guard;
    }
    EXECUTING.store(false, Ordering::SeqCst);
    ADBC_STATUS_CANCELLED
}

unsafe extern "C" fn statement_cancel(
    _statement: *mut adbc_ffi::FFI_AdbcStatement,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    let mut cancelled = CANCELLED
        .lock()
        .expect("the cancel state should be lockable");
    *cancelled = true;
    CANCEL_SIGNAL.notify_all();
    ADBC_STATUS_OK
}

unsafe extern "C" fn driver_release(
    _driver: *mut adbc_ffi::FFI_AdbcDriver,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    ADBC_STATUS_OK
}

unsafe extern "C" fn driver_init(
    _version: c_int,
    raw_driver: *mut c_void,
    _error: *mut adbc_ffi::FFI_AdbcError,
) -> AdbcStatusCode {
    let driver = raw_driver.cast::<adbc_ffi::FFI_AdbcDriver>();
    unsafe {
        (*driver).release = Some(driver_release);
        (*driver).DatabaseNew = Some(noop_database);
        (*driver).DatabaseInit = Some(noop_database);
        (*driver).DatabaseRelease = Some(noop_database);
        (*driver).ConnectionNew = Some(connection_new);
        (*driver).ConnectionInit = Some(connection_init);
        (*driver).ConnectionRelease = Some(connection_release);
        (*driver).StatementNew = Some(statement_new);
        (*driver).StatementRelease = Some(statement_release);
        (*driver).StatementSetSqlQuery = Some(statement_set_sql_query);
        (*driver).StatementExecuteQuery = Some(statement_execute_query);
        (*driver).StatementCancel = Some(statement_cancel);
    }
    ADBC_STATUS_OK
}

/// A cancel issued while `execute` is blocked must be delivered to the driver
/// straight away, not once `execute` has returned.
#[test]
fn cancel_reaches_a_running_execute() {
    let _serialized = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reset_state();

    let init: adbc_ffi::FFI_AdbcDriverInitFunc = driver_init;
    let mut driver =
        ManagedDriver::load_static(&init, AdbcVersion::V110).expect("the driver should load");
    let database = driver
        .new_database()
        .expect("the database should be created");
    let mut connection = database
        .new_connection()
        .expect("the connection should be established");
    let mut statement = connection
        .new_statement()
        .expect("the statement should be allocated");
    statement
        .set_sql_query("SELECT 1")
        .expect("the query should be set");

    let mut canceller = statement.clone();
    let executor = std::thread::spawn(move || statement.execute().err().map(|e| e.status));

    // Cancel a call that has actually started, so a pass cannot come from
    // cancelling before the driver ever blocked.
    let started = Instant::now();
    while !EXECUTING.load(Ordering::SeqCst) {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the driver never entered StatementExecuteQuery"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let cancel_started = Instant::now();
    canceller.cancel().expect("the cancel should be accepted");
    let cancel_took = cancel_started.elapsed();

    let execute_status = executor
        .join()
        .expect("the executing thread should not panic");

    assert!(
        cancel_took < Duration::from_secs(5),
        "cancel took {cancel_took:?}, so it waited for the execute it was meant to interrupt"
    );
    assert_eq!(
        execute_status,
        Some(Status::Cancelled),
        "the driver did not see the cancel while the query was running"
    );
}

/// A cloned handle is another reference to one statement, not a second
/// statement: dropping one must not release the statement the other is using.
#[test]
fn dropping_one_handle_leaves_the_other_usable() {
    let _serialized = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reset_state();

    let init: adbc_ffi::FFI_AdbcDriverInitFunc = driver_init;
    let mut driver =
        ManagedDriver::load_static(&init, AdbcVersion::V110).expect("the driver should load");
    let database = driver
        .new_database()
        .expect("the database should be created");
    let mut connection = database
        .new_connection()
        .expect("the connection should be established");
    let statement = connection
        .new_statement()
        .expect("the statement should be allocated");

    let mut survivor = statement.clone();
    drop(statement);

    survivor
        .set_sql_query("SELECT 1")
        .expect("the surviving handle should still address a live statement");
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        0,
        "the statement was released while a handle to it was still in use"
    );

    drop(survivor);
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        1,
        "one statement must be released exactly once"
    );
}
