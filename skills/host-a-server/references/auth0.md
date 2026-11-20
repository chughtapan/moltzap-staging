# Auth0

Each line was observed on a live tenant on the date given.

- **The API's identifier is the server's resource URL, exactly**, signed with
  RS256, with the scope as a permission. Tokens then carry it as audience and
  the daemon admits them. (2026-11-19)
- **Hosts register themselves** when the tenant setting
  `enable_dynamic_client_registration` is on. A registration made this way is
  a third-party client. (2026-11-19)
- **A new API refuses every self-registered host until it has a default
  grant.** Without it sign-in stops at Auth0 with "Client is not authorized to
  access resource server". The grant, through the Management API:
  `POST /api/v2/client-grants` with
  `{"audience": "<resource>", "scope": ["<scope>"], "subject_type": "user", "default_for": "third_party_clients"}`.
  The `auth0 client-grants create --default-for` command cannot set
  `subject_type`, so use `auth0 api post client-grants --data ...`. With the
  grant, a self-registered host signed in through Google and the daemon
  admitted its token. (2026-11-19)
- **Hosts send `resource`, not Auth0's `audience`.** The tenant setting
  `resource_parameter_profile: compatibility` makes Auth0 accept it. (2026-11-19)
- **A free tenant holds ten applications**, and every self-registered host is
  one. At the limit, creating one fails with "You reached the limit of
  entities of this type for this tenant". (2026-11-19)
- **A self-registered host's token cannot read the user's name or email.**
  Auth0 issues it for the API only and its user-info endpoint answers 401.
  (2026-11-19)
