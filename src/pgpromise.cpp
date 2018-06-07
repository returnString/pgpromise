#define STRICT_R_HEADERS
#include <Rcpp.h>

#include <later_api.h>
#include <libpq-fe.h>
#include <postgres.h>
#include <catalog/pg_type.h>

#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <memory>
#include <map>
#include <atomic>
#include <string>

struct ConnectionConfig
{
	std::string host;
	std::string port;
	std::string db;
	std::string user;
	std::string password;
	int workers;
};

struct FieldResult
{
	std::string value;
	bool null;
};

struct ColumnResult
{
	Oid type;
	std::string name;
	std::vector<FieldResult> values;
};

struct QueryIntermediateResult
{
	bool succeeded;
	std::vector<ColumnResult> columns;
	std::string errorMessage;
};

struct QueryRequest
{
	std::string queryText;
	Rcpp::Function resolve;
	Rcpp::Function reject;
	QueryIntermediateResult result;

	QueryRequest(std::string query, Rcpp::Function res, Rcpp::Function rej)
		: queryText(query)
		, resolve(res)
		, reject(rej)
	{
	}
};

class ConnectionPool
{
public:
	ConnectionPool(ConnectionConfig config)
		: m_config(config)
	{
		for (int i = 0; i < m_config.workers; ++i)
		{
			m_threads.push_back(std::thread([this] { ThreadLoop(); }));
		}
	}
	
	~ConnectionPool()
	{
		m_shuttingDown = true;
		m_requestCondVar.notify_all();
		
		for (auto& thread : m_threads)
		{
			thread.join();
		}
	}
	
	void Exec(std::unique_ptr<QueryRequest> req)
	{
		{
			std::lock_guard<std::mutex> lock(m_requestMutex);
			m_requests.push(std::move(req));
		}
		
		m_requestCondVar.notify_one();
	}
	
private:
	void ThreadLoop()
	{
		auto conn = PQsetdbLogin(m_config.host.c_str(), m_config.port.c_str(),
      nullptr, nullptr,
    	m_config.db.c_str(), m_config.user.c_str(), m_config.password.c_str());
		
		// TODO: verify this call actually succeeds and propagate error back to R thread
		auto setRes = PQexec(conn, "set datestyle to ISO,YMD");
		PQclear(setRes);
		
		std::unique_ptr<QueryRequest> req;
		while (WaitForRequest(req))
		{
			auto res = PQexec(conn, req->queryText.c_str());
			auto status = PQresultStatus(res);
			
			QueryIntermediateResult intermed;
			if (status == PGRES_TUPLES_OK)
			{
				intermed.succeeded = true;
				
				auto rowCount = PQntuples(res);
				auto columnCount = PQnfields(res);
				
				for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex)
				{
					ColumnResult column;
					column.name = PQfname(res, columnIndex);
					column.type = PQftype(res, columnIndex);
					column.values.reserve(rowCount);
					
					for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex)
					{
						FieldResult field;
						field.null = static_cast<bool>(PQgetisnull(res, rowIndex, columnIndex));
						if (!field.null)
						{
							field.value = PQgetvalue(res, rowIndex, columnIndex);
						}
						
						column.values.push_back(field);
					}
					
					intermed.columns.push_back(column);
				}
			}
			else
			{
				intermed.succeeded = false;
				intermed.errorMessage = PQresultErrorMessage(res);
			}
			
			req->result = intermed;
			
			PQclear(res);
			later::later(QueryCompleteCallback, req.release(), 0);
		}
		
		PQfinish(conn);
	}
	
	static Rcpp::RObject TranslateColumn(const ColumnResult& column)
	{
		switch (column.type)
		{
			case INT2OID:
			case INT4OID:
			case INT8OID:
			{
				Rcpp::NumericVector numVec(column.values.size());
				for (size_t i = 0; i < column.values.size(); ++i)
				{
					const auto& field = column.values[i];
					if (field.null)
					{
						numVec[i] = NA_REAL;
					}
					else
					{
						numVec[i] = std::stol(field.value);
					}
				}
				return numVec;
			}
			break;
				
			case FLOAT4OID:
			case FLOAT8OID:
			case NUMERICOID:
			{
				Rcpp::NumericVector numVec(column.values.size());
				for (size_t i = 0; i < column.values.size(); ++i)
				{
					const auto& field = column.values[i];
					if (field.null)
					{
						numVec[i] = NA_REAL;
					}
					else
					{
						numVec[i] = std::stod(field.value);
					}
				}
				return numVec;
			}
			break;
			
			case TEXTOID:
			case BPCHAROID:
			case VARCHAROID:
			{
				Rcpp::CharacterVector charVec(column.values.size());
				for (size_t i = 0; i < column.values.size(); ++i)
				{
					const auto& field = column.values[i];
					if (field.null)
					{
						charVec[i] = NA_STRING;
					}
					else
					{
						charVec[i] = field.value;
					}
				}
				return charVec;
			}
			break;
				
			case TIMESTAMPOID:
			{
				Rcpp::DatetimeVector dateTimeVec(column.values.size());
				for (size_t i = 0; i < column.values.size(); ++i)
				{
					const auto& field = column.values[i];
					if (field.null)
					{
						dateTimeVec[i] = Rcpp::DatetimeVector::get_na();
					}
					else
					{
						dateTimeVec[i] = Rcpp::Datetime(field.value);
					}
				}
				dateTimeVec.attr("class") = "POSIXct";
				return dateTimeVec;
			}
			break;
				
			case DATEOID:
			{
				Rcpp::DateVector dateVec(column.values.size());
				for (size_t i = 0; i < column.values.size(); ++i)
				{
					const auto& field = column.values[i];
					if (field.null)
					{
						dateVec[i] = Rcpp::DateVector::get_na();
					}
					else
					{
						dateVec[i] = Rcpp::Date(field.value);
					}
				}
				dateVec.attr("class") = "Date";
				return dateVec;
			}
			break;
				
			case BOOLOID:
			{
				Rcpp::LogicalVector boolVec(column.values.size());
				for (size_t i = 0; i < column.values.size(); ++i)
				{
					const auto& field = column.values[i];
					if (field.null)
					{
						boolVec[i] = NA_LOGICAL;
					}
					else
					{
						boolVec[i] = field.value == "t";
					}
				}
				return boolVec;
			}
			break;
				
			default:
			{
				Rf_warning("unhandled oid %d for column %s\n", column.type, column.name.c_str());
				return nullptr;
			}
			break;
		}
	}
	
	static void QueryCompleteCallback(void* data)
	{
		auto req = std::unique_ptr<QueryRequest>(static_cast<QueryRequest*>(data));
		
		if (req->result.succeeded)
		{
			auto df = Rcpp::DataFrame::create();

			for (const auto& column : req->result.columns)
			{
				if (auto translatedColumn = TranslateColumn(column))
				{
					df.push_back(translatedColumn, column.name);
				}
			}
			
			req->resolve(df);
		}
		else
		{
			req->reject(req->result.errorMessage);
		}
	}
	
	bool WaitForRequest(std::unique_ptr<QueryRequest>& req)
	{
		std::unique_lock<std::mutex> lock(m_requestMutex);
		while (m_requests.empty())
		{
			if (m_shuttingDown)
			{
				return false;
			}
			
			m_requestCondVar.wait(lock);
		}
		
		req = std::move(m_requests.front());
		m_requests.pop();
		return true;
	}
	
	ConnectionConfig m_config;
	std::mutex m_requestMutex;
	std::atomic<bool> m_shuttingDown { false };
	std::condition_variable m_requestCondVar;
	std::queue<std::unique_ptr<QueryRequest>> m_requests;
	std::vector<std::thread> m_threads;
};

// [[Rcpp::export]]
void attach_pool(Rcpp::RObject conn, std::string host, int port,
            std::string db, std::string user, std::string password, int workers)
{
	ConnectionConfig config;
	config.host = host;
	config.port = std::to_string(port);
	config.db = db;
	config.user = user;
	config.password = password;
	config.workers = workers;
	
	Rcpp::XPtr<ConnectionPool> pool(new ConnectionPool(config));
	conn.attr("pgpromiseconn") = pool;
}

// [[Rcpp::export]]
void collect_async_callback(Rcpp::RObject conn, Rcpp::CharacterVector query, Rcpp::Function resolve, Rcpp::Function reject)
{
	auto queryText = Rcpp::as<std::string>(query);
	Rcpp::XPtr<ConnectionPool> pool = conn.attr("pgpromiseconn");
	pool->Exec(std::unique_ptr<QueryRequest>(new QueryRequest(queryText, resolve, reject)));
}