import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    account_read_stress: {
      executor: 'ramping-arrival-rate',

      startRate: 100,
      timeUnit: '1s',

      preAllocatedVUs: 50,
      maxVUs: 500,

      stages: [
        { target: 100, duration: '30s' },
        { target: 250, duration: '30s' },
        { target: 500, duration: '30s' },
        { target: 100, duration: '30s' },
      ],
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