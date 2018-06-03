library(dplyr)
library(magrittr)
library(promises)

conn <- create_postgres_pool(
	host = "localhost",
	port = 5432,
	db = "ruan",
	user = "ruan",
	password = "",
	workers = 8
)

test_table <- tbl(conn, "rtest")
truncate_timestamp_expr <- sql("tz_col_max::date")
test_table %>%
	group_by(text_col) %>% 
	summarise(int_col_total = sum(int_col, na.rm = T),
						float_col_total = sum(float_col, na.rm = T),
						num_col_total = sum(num_col, na.rm = T),
						tz_col_max = max(tz_col, na.rm = T),
						bool_any = any(bool_col, na.rm = T),
						char = string_agg(char_col, ","),
						vchar = string_agg(vchar_col, ",")) %>%
	mutate(tz_truncated = truncate_timestamp_expr) %>%
	collect_async() %...>%
	{
		str(.)
		print(.)
	}
