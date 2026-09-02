import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    account_reads: {
      executor: 'constant-arrival-rate',
      rate: 100,
      timeUnit: '1s',
      duration: '1m',
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
};

export default function () {

  const url =
    'http://localhost:8081/api/v1/accounts/303800890340';

  const res = http.get(url);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });
}