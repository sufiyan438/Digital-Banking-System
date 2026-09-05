//import http from 'k6/http';
//import exec from 'k6/execution';
//import { check } from 'k6';
//import { SharedArray } from 'k6/data';
//import papaparse from 'https://jslib.k6.io/papaparse/5.1.1/index.js';
//
//const accounts = new SharedArray('accounts', function () {
//  return papaparse
//    .parse(open('./loadtestingAccounts.csv'), { header: true })
//    .data
//    .filter((row) => row.account_number);
//});
//
//export const options = {
//  scenarios: {
//    transfers: {
//      executor: 'constant-arrival-rate',
//      rate: 40,
//      timeUnit: '1s',
//      duration: '1m',
//      preAllocatedVUs: 100,
//      maxVUs: 300,
//    },
//  },
//};
//
//export default function () {
//  const i = exec.scenario.iterationInTest;
//
//  const senderIndex = i % 2000;
//  const receiverIndex = (senderIndex + 1000) % 2000;
//
//  const sender = accounts[senderIndex].account_number;
//  const receiver = accounts[receiverIndex].account_number;
//
//  const url = 'http://localhost:8082/api/v1/transactions/transfer';
//
//  const payload = JSON.stringify({
//    senderAccountNumber: sender,
//    recieverAccountNumber: receiver,
//    amount: 10
//  });
//
//  const params = {
//    headers: {
//      'Content-Type': 'application/json',
//    },
//  };
//
//  const res = http.post(url, payload, params);
//
//  check(res, {
//    'transfer accepted': (r) => r.status === 201,
//  });
//
//  if (res.status !== 201) {
//    console.log(
//      `FAILED sender=${sender} receiver=${receiver} status=${res.status} body=${res.body}`
//    );
//  }
//}



import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Trend, Counter } from 'k6/metrics';
import papaparse from 'https://jslib.k6.io/papaparse/5.1.1/index.js';


// -------------------------------------------------------
// CUSTOM METRICS
// -------------------------------------------------------

// Only POST /transfer latency
const transferApiDuration =
    new Trend('transfer_api_duration_ms');

// Complete Saga:
// POST request -> final transaction state
const sagaDuration =
    new Trend('saga_duration_ms');

const sagaCompleted =
    new Counter('saga_completed');

const sagaRefunded =
    new Counter('saga_refunded');

const sagaPendingVerification =
    new Counter('saga_pending_verification');

const sagaTimedOut =
    new Counter('saga_timed_out');

const transferAccepted =
    new Counter('transfer_accepted');

const transferFailed =
    new Counter('transfer_failed');


// -------------------------------------------------------
// LOAD TEST ACCOUNTS
// -------------------------------------------------------

const accounts = new SharedArray('accounts', function () {

    return papaparse
        .parse(
            open('./loadtestingAccounts.csv'),
            { header: true }
        )
        .data
        .filter((row) => row.account_number);

});


// -------------------------------------------------------
// TEST CONFIGURATION
// -------------------------------------------------------

export const options = {

    scenarios: {

        transfers: {

            executor: 'constant-arrival-rate',

            // CLEAN BASELINE
            rate: 55,

            timeUnit: '1s',

            duration: '1m',

            /*
             * Our Saga currently takes roughly 12-16 seconds.
             *
             * At 10 transfers/sec:
             *
             * 10 × ~16 sec = ~160 simultaneously active VUs
             *
             * Give k6 plenty of headroom.
             */
            preAllocatedVUs: 1500,

            maxVUs: 1800,

            // Allow in-flight Sagas to finish
            gracefulStop: '45s',
        },
    },


    thresholds: {

        // No meaningful HTTP failures
        http_req_failed: [
            'rate<0.01',
        ],

        // POST /transfer performance
        transfer_api_duration_ms: [
            'p(95)<100',
        ],

        // Complete Saga performance
        saga_duration_ms: [
            'p(95)<20000',
        ],
    },
};


// -------------------------------------------------------
// CONFIG
// -------------------------------------------------------

const BASE_URL =
    'http://localhost:8082';

const TRANSFER_URL =
    `${BASE_URL}/api/v1/transactions/transfer`;

const MAX_SAGA_WAIT_SECONDS =
    30;

const POLL_INTERVAL_SECONDS =
    1;


// -------------------------------------------------------
// TEST
// -------------------------------------------------------

export default function () {

    const i =
        exec.scenario.iterationInTest;


    // ---------------------------------------------------
    // PICK ACCOUNTS
    // ---------------------------------------------------

    const senderIndex =
        i % accounts.length;

    const receiverIndex =
        (senderIndex + 1000) % accounts.length;


    const sender =
        accounts[senderIndex].account_number;

    const receiver =
        accounts[receiverIndex].account_number;


    // ---------------------------------------------------
    // CREATE TRANSFER
    // ---------------------------------------------------

    const payload = JSON.stringify({

        senderAccountNumber:
            sender,

        recieverAccountNumber:
            receiver,

        amount:
            10,

        description:
            'Load test transfer',
    });


    const params = {

        headers: {
            'Content-Type':
                'application/json',
        },

        tags: {
            request_type:
                'transfer',
        },
    };


    // Whole Saga starts here
    const sagaStart =
        Date.now();


    // ---------------------------------------------------
    // CALL TRANSFER API
    // ---------------------------------------------------

    const res = http.post(
        TRANSFER_URL,
        payload,
        params
    );


    // Record ONLY transfer POST latency
    transferApiDuration.add(
        res.timings.duration
    );


    const accepted = check(res, {

        'transfer accepted':
            (r) => r.status === 201,

    });


    // ---------------------------------------------------
    // TRANSFER REJECTED
    // ---------------------------------------------------

    if (!accepted) {

        transferFailed.add(1);

        console.log(
            `TRANSFER FAILED ` +
            `sender=${sender} ` +
            `receiver=${receiver} ` +
            `status=${res.status} ` +
            `body=${res.body}`
        );

        return;
    }


    transferAccepted.add(1);


    // ---------------------------------------------------
    // GET TRANSACTION ID
    // ---------------------------------------------------

    let responseBody;


    try {

        responseBody =
            res.json();

    }
    catch (e) {

        console.log(
            `Could not parse transfer response: ` +
            `${res.body}`
        );

        return;
    }


    const transactionId =
        responseBody.id;


    if (!transactionId) {

        console.log(
            `No transaction ID returned: ` +
            `${res.body}`
        );

        return;
    }


    // Transaction normally begins as PROCESSING
    let finalStatus =
        responseBody.status;


    // ---------------------------------------------------
    // POLL UNTIL SAGA FINISHES
    // ---------------------------------------------------

    const maxPolls =
        Math.ceil(
            MAX_SAGA_WAIT_SECONDS /
            POLL_INTERVAL_SECONDS
        );


    for (
        let poll = 0;
        poll < maxPolls;
        poll++
    ) {


        // -------------------------------
        // STOP IF FINAL/INTERESTING STATE
        // -------------------------------

        if (
            finalStatus === 'COMPLETED' ||
            finalStatus === 'REFUNDED' ||
            finalStatus === 'PENDING_VERIFICATION'
        ) {

            break;
        }


        // Wait before asking again
        sleep(
            POLL_INTERVAL_SECONDS
        );


        // -------------------------------
        // GET CURRENT TRANSACTION STATUS
        // -------------------------------

        const statusRes =
            http.get(

                `${BASE_URL}/api/v1/transactions/${transactionId}`,

                {
                    tags: {
                        request_type:
                            'saga_status_poll',
                    },
                }
            );


        if (
            statusRes.status !== 200
        ) {

            continue;
        }


        try {

            finalStatus =
                statusRes.json().status;

        }
        catch (e) {

            continue;
        }
    }


    // ---------------------------------------------------
    // WHOLE SAGA LATENCY
    // ---------------------------------------------------

    const totalSagaTime =
        Date.now() - sagaStart;


    // ---------------------------------------------------
    // FINAL RESULT
    // ---------------------------------------------------

    if (
        finalStatus === 'COMPLETED'
    ) {

        sagaDuration.add(
            totalSagaTime
        );

        sagaCompleted.add(1);

    }


    else if (
        finalStatus === 'REFUNDED'
    ) {

        sagaDuration.add(
            totalSagaTime
        );

        sagaRefunded.add(1);

        console.log(
            `Transaction ${transactionId} ` +
            `was REFUNDED after ` +
            `${totalSagaTime} ms`
        );

    }


    else if (
        finalStatus ===
        'PENDING_VERIFICATION'
    ) {

        sagaPendingVerification.add(1);

        console.log(
            `Transaction ${transactionId} ` +
            `requires OTP verification`
        );

    }


    else {

        sagaTimedOut.add(1);

        console.log(
            `Transaction ${transactionId} ` +
            `did not finish within ` +
            `${MAX_SAGA_WAIT_SECONDS}s. ` +
            `Last status=${finalStatus}`
        );

    }
}