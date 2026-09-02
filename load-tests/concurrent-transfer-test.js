import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 20,
  iterations: 20,
};

export default function () {
  const url = 'http://localhost:8082/api/v1/transactions/transfer';

  const payload = JSON.stringify({
    senderAccountNumber: '303800890340',
    recieverAccountNumber: '424716442762',
    amount: 1000
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);
  console.log(`STATUS: ${res.status} BODY: ${res.body}`);

  check(res, {
    'accepted or correctly rejected': (r) =>
      r.status === 201 || r.status === 400,
  });
}