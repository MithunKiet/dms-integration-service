# dms-integration-service

## API clients from `.env`

You can configure API clients directly from `.env` using `API_KEY_SETTINGS` as a JSON string.

```env
API_KEY_SETTINGS={"clients":[{"client_id":"mobile-app","client_name":"Mobile Application","api_key":"mob_live_a1b2c3d4e5f6g7h8i9j0","is_active":true,"allowed_endpoints":[]},{"client_id":"web-portal","client_name":"Web Portal","api_key":"web_live_x9y8z7w6v5u4t3s2r1q0","is_active":true,"allowed_endpoints":[]},{"client_id":"dms-system","client_name":"ERP Integration","api_key":"dms_k1l2m3n4o5p6q7r8s9t0","is_active":true,"allowed_endpoints":["/api/v1/customers/by-sap-customer-code","/api/v1/customers/from-sap-file/by-sap-customer-code","/api/v1/inventory/distributor-inventory-ageing","/api/v1/material/material-master","api/v1/hmis/sap-price"]},{"client_id":"partner-api","client_name":"Partner API","api_key":"par_live_u1v2w3x4y5z6a7b8c9d0","is_active":false,"allowed_endpoints":[]},{"client_id":"b2b_bb","client_name":"B2B Backend Integration","api_key":"8f3Kp9Lm2Qa7Xv1Rr6Ty","is_active":true,"allowed_endpoints":["/api/v1/inventory/distributor-inventory-ageing"]}]}
```

Notes:
- `allowed_endpoints: []` means no endpoint access.
- Use `"*"` inside `allowed_endpoints` to allow all endpoints.
- Endpoints missing a leading `/` are automatically normalized.
