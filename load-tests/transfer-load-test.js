import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import papaparse from 'https://jslib.k6.io/papaparse/5.1.1/index.js';

const accounts = new SharedArray('accounts', function () {
  return papaparse
    .parse(open('./loadtestingAccounts.csv'), { header: true })
    .data
    .filter((row) => row.account_number);
});

export const options = {
  scenarios: {
    transfers: {
      executor: 'constant-arrival-rate',
      rate: 40,
      timeUnit: '1s',
      duration: '1m',
      preAllocatedVUs: 100,
      maxVUs: 300,
    },
  },
};

export default function () {
  const i = exec.scenario.iterationInTest;

  const senderIndex = i % 2000;
  const receiverIndex = (senderIndex + 1000) % 2000;

  const sender = accounts[senderIndex].account_number;
  const receiver = accounts[receiverIndex].account_number;

  const url = 'http://localhost:8082/api/v1/transactions/transfer';

  const payload = JSON.stringify({
    senderAccountNumber: sender,
    recieverAccountNumber: receiver,
    amount: 10
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'transfer accepted': (r) => r.status === 201,
  });

  if (res.status !== 201) {
    console.log(
      `FAILED sender=${sender} receiver=${receiver} status=${res.status} body=${res.body}`
    );
  }
}