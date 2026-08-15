//! GraphQL API over the indexed data.

pub mod scalars;
pub mod schema;

pub use schema::{CoreQuery, IndexerSchema, build_schema, build_schema_with};

use async_graphql::http::GraphiQLSource;
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

async fn graphql_handler(
    State(schema): State<IndexerSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn health() -> impl IntoResponse {
    "ok"
}

/// Build the HTTP router: GraphiQL at `/`, the API at `/graphql`, health at `/health`.
pub fn router(pool: PgPool) -> Router {
    let schema = build_schema(pool);

    Router::new()
        .route("/", get(graphiql))
        .route("/graphql", get(graphiql).post(graphql_handler))
        .route("/health", get(health))
        .with_state(schema)
}
