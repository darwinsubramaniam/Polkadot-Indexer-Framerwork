//! GraphQL API over the indexed data.

pub mod scalars;
pub mod schema;

pub use schema::{CoreQuery, IndexerSchema, build_schema, build_schema_with};

use async_graphql::{EmptyMutation, EmptySubscription, Schema, http::GraphiQLSource};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
};
use sqlx::PgPool;

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

async fn graphql_handler<Q>(
    State(schema): State<Schema<Q, EmptyMutation, EmptySubscription>>,
    req: GraphQLRequest,
) -> GraphQLResponse
where
    Q: async_graphql::ObjectType + 'static,
{
    schema.execute(req.into_inner()).await.into()
}

async fn health() -> impl IntoResponse {
    "ok"
}

/// Build the HTTP router over the framework schema: GraphiQL at `/`, the API at `/graphql`,
/// health at `/health`.
pub fn router(pool: PgPool) -> Router {
    router_with(build_schema(pool))
}

/// The same routes over a schema you built yourself.
///
/// [`build_schema_with`] lets a project merge its own query root into the framework's, but
/// without this the merged schema could never actually be served — [`router`] hard-wired
/// [`CoreQuery`], so every downstream project had to reimplement the router to expose one
/// extra field.
///
/// ```ignore
/// #[derive(async_graphql::MergedObject, Default)]
/// struct Query(pif_api::CoreQuery, pif_identity::IdentityQuery);
///
/// let schema = pif_api::build_schema_with(pool, Query::default());
/// let app = pif_api::router_with(schema);
/// ```
pub fn router_with<Q>(schema: Schema<Q, EmptyMutation, EmptySubscription>) -> Router
where
    Q: async_graphql::ObjectType + 'static,
{
    Router::new()
        .route("/", get(graphiql))
        .route("/graphql", get(graphiql).post(graphql_handler::<Q>))
        .route("/health", get(health))
        .with_state(schema)
}
