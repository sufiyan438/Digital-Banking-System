import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 1,
  iterations: 1900,
};

export default function () {

  // __ITER goes 100–1999
  const i = __ITER +100;

  const url = 'http://localhost:8081/api/v1/accounts';

  const payload = JSON.stringify({
    accountHolderName: `Load Test User ${i}`,
    email: `loadtest${i}@testmail.com`,
    phone: `900000${String(i).padStart(4, '0')}`,
    accountType: 'SAVINGS',
    initialDeposit: 100000
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  console.log(`USER ${i} STATUS ${res.status} BODY ${res.body}`);

  check(res, {
    'account created': (r) => r.status === 200 || r.status === 201,
  });
}