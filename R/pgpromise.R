
create_postgres_pool <- function(host, port, db, user, password, workers)
{
	conn <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), host = host, port = port,
												 dbname = db, user = user, password = password)
	
	.Call(`_pgpromise_attach_pool`, conn, host, port, db, user, password, workers)
	conn
}

collect_async <- function(query)
{
	sql <- dbplyr::sql_render(query)
	promises::promise(function(resolve, reject)
	{
		.Call(`_pgpromise_collect_async_callback`, query$src$con, sql,
			function(x) resolve(as.data.frame(x, stringsAsFactors = F)), reject)
	})
}

