using System.Data;
using System.Data.Common;
using System.Text;

using CodedThought.Core.Data.Interfaces;
using CodedThought.Core.Exceptions;

using MySqlConnector;

namespace CodedThought.Core.Data.MySql
{
    /// <summary>MySqlDatabaseObject provides all MySql specific functionality needed by DBStore and its family of classes.</summary>
    public class MySqlDatabaseObject : DatabaseObject, IDatabaseObject, IDbSchema
    {
        #region Declarations

        private MySqlConnection _connection;

        #endregion Declarations

        #region Constructor

        public MySqlDatabaseObject() : base() => _connection = new();

        #endregion Constructor

        #region Transaction and Connection Methods

        /// <summary>
        /// Returns the active connection. If the stack has a connection then it is returned.
        /// connection is created.
        /// </summary>
        public override IDbConnection Connection => _connection == null ? (MySqlConnection) base.Connection : (MySqlConnection) _connection;

        /// <summary>
        /// Opens a MySql Connection
        /// </summary>
        /// <returns></returns>
        protected override IDbConnection OpenConnection()
        {
            try
            {
                if (string.IsNullOrEmpty(_connection.ConnectionString))
                    _connection = new(ConnectionString);
                if (_connection.State != ConnectionState.Open)
                    _connection.Open();
                return _connection;
            }
            catch (MySqlException ex)
            {
                throw new ApplicationException("Could not open Connection.  Check connection string" + "/r/n" + ex.Message + "/r/n" + ex.StackTrace, ex);
            }
        }

        /// <summary>
        /// Opens a MySql Connection asynchronously.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        protected override async Task<IDbConnection> OpenConnectionAsync()
        {
            try
            {
                _connection = new MySqlConnection(ConnectionString);
                await _connection.OpenAsync();
                return _connection;
            }
            catch (MySqlException ex)
            {
                throw new ApplicationException("Could not open Connection.  Check connection string" + "/r/n" + ex.Message + "/r/n" + ex.StackTrace, ex);
            }
        }
        /// <summary>
        /// Tests the connection to the database.
        /// </summary>
        /// <returns></returns>
        public override bool TestConnection()
        {
            try
            {
                OpenConnection();
                return Connection.State == ConnectionState.Open;
            }
            catch (CodedThoughtException)
            {
                throw;
            }
        }
        /// <summary>
        /// Commits updates and inserts.  This is only for Oracle database operations.
        /// </summary>
        public override void Commit() => ExecuteNonQuery("COMMIT", CommandType.Text);

        #endregion Transaction and Connection Methods

        #region Other Override Methods

        /// <summary>
        /// Creates a Sql Data Adapter object with the passed Command object.
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        protected override IDataAdapter CreateDataAdapter(IDbCommand cmd) => new MySqlDataAdapter(cmd as MySqlCommand);

        /// <summary>Convert any data type to Char</summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public override string ConvertToChar(string columnName) => "CONVERT(varchar, " + columnName + ")";

        public override IDataParameter CreateApiParameter(string paraemterName, string parameterValue) => throw new NotImplementedException();

        /// <summary>Creates the parameter collection.</summary>
        /// <returns></returns>
        public override ParameterCollection CreateParameterCollection() => new(this);

        /// <summary>
        /// Test the connection using an asyncronous process.
        /// </summary>
        /// <returns></returns>
        public override async Task<bool> TestConnectionAsync()
        {
            try
            {
                await OpenConnectionAsync();
                return Connection.State == ConnectionState.Open;
            }
            catch { throw; }
        }

        #region Parameters

        /// <summary>Gets the column delimiter character.</summary>
        public override string ColumnDelimiter => throw new NotImplementedException();

        /// <summary>Returns the param connector for MySql, @</summary>
        /// <returns></returns>
        public override string ParameterConnector => "@";

        /// <summary>Gets the wild card character.</summary>
        /// <value>The wild card character.</value>
        public override string WildCardCharacter => "%";

        /// <summary>Creates the SQL server param.</summary>
        /// <param name="srcTableColumnName">Name of the SRC table column.</param>
        /// <param name="paramType">         Type of the param.</param>
        /// <returns></returns>
        private MySqlParameter CreateDbServerParam(string srcTableColumnName, MySqlDbType paramType)
        {
            MySqlParameter param = new(ToSafeParamName(srcTableColumnName), paramType)
            {
                SourceColumn = srcTableColumnName
            };
            return param;
        }

        /// <summary>Creates the SQL server param.</summary>
        /// <param name="srcTableColumnName">Name of the SRC table column.</param>
        /// <param name="paramType">         Type of the param.</param>
        /// <param name="size">              The size.</param>
        /// <returns></returns>
        private MySqlParameter CreateDbServerParam(string srcTableColumnName, MySqlDbType paramType, int size)
        {
            MySqlParameter param = new(ToSafeParamName(srcTableColumnName), paramType, size)
            {
                SourceColumn = srcTableColumnName
            };
            return param;
        }

        public override IDataParameter CreateBetweenParameter(string srcTableColumnName, BetweenParameter betweenParam) => throw new NotImplementedException();

        /// <summary>Creates a Blob parameter based on supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <param name="size">              </param>
        /// <returns></returns>
        public IDataParameter CreateBlobParameter(string srcTableColumnName, byte[] parameterValue, int size)
        {
            IDataParameter returnValue = CreateDbServerParam(srcTableColumnName, MySqlDbType.Blob, size);
            returnValue.Value = parameterValue;

            return returnValue;
        }

        /// <summary>Creates a boolean parameter.</summary>
        /// <param name="srcTaleColumnName">Name of the SRC tale column.</param>
        /// <param name="parameterValue">   The parameter value.</param>
        /// <returns></returns>
        public override IDataParameter CreateBooleanParameter(string srcTableColumnName, bool parameterValue)
        {
            IDataParameter returnValue = CreateEmptyParameter();

            returnValue = CreateDbServerParam(srcTableColumnName, MySqlDbType.Bool);
            returnValue.Value = parameterValue;
            return returnValue;
        }

        /// <summary>Creates a Char parameter based on supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <param name="size">              </param>
        /// <returns></returns>
        public override IDataParameter CreateCharParameter(string srcTableColumnName, string parameterValue, int size)
        {
            IDataParameter returnValue = CreateDbServerParam(srcTableColumnName, MySqlDbType.VarChar);
            returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Create a data time parameter based on supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateDateTimeParameter(string srcTableColumnName, DateTime parameterValue)
        {
            IDataParameter returnValue = CreateDbServerParam(srcTableColumnName, MySqlDbType.DateTime);
            returnValue.Value = parameterValue != DateTime.MinValue ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates a Double parameter based on supported database</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateDoubleParameter(string srcTableColumnName, double parameterValue)
        {
            IDataParameter returnValue = CreateDbServerParam(srcTableColumnName, MySqlDbType.Double);
            returnValue.Value = parameterValue != double.MinValue ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Create an empty parameter for MySql</summary>
        /// <returns></returns>
        public override IDataParameter CreateEmptyParameter() => new MySqlParameter();

        /// <summary>Creates the GUID parameter.</summary>
        /// <param name="srcTableColumnName">Name of the SRC table column.</param>
        /// <param name="parameterValue">    The parameter value.</param>
        /// <returns></returns>
        public override IDataParameter CreateGuidParameter(string srcTableColumnName, Guid parameterValue)
        {
            IDataParameter returnValue = CreateDbServerParam(srcTableColumnName, MySqlDbType.Guid);
            returnValue.Value = parameterValue;

            return returnValue;
        }

        /// <summary>Creates a Int32 parameter for the supported database</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateInt32Parameter(string srcTableColumnName, int parameterValue)
        {
            IDataParameter returnValue = CreateDbServerParam(srcTableColumnName, MySqlDbType.Int32);
            returnValue.Value = parameterValue != int.MinValue ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>
        /// Creates the output parameter.
        /// </summary>
        /// <param name="parameterName">Name of the parameter.</param>
        /// <param name="returnType">Type of the return.</param>
        /// <returns></returns>
        /// <exception cref="ApplicationException">Data type not supported.  DataTypes currently suported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar</exception>
        public override IDataParameter CreateOutputParameter(string parameterName, DbTypeSupported returnType)
        {
            MySqlDbType sqlDataType;
            switch (returnType)
            {
                case DbTypeSupported.dbVarChar:
                case DbTypeSupported.dbChar:
                case DbTypeSupported.dbNVarChar:
                    sqlDataType = MySqlDbType.VarChar;
                    break;

                case DbTypeSupported.dbTinyInt:
                case DbTypeSupported.dbInt16:
                    sqlDataType = MySqlDbType.Int16;
                    break;

                case DbTypeSupported.dbInt32:
                    sqlDataType = MySqlDbType.Int32;
                    break;

                case DbTypeSupported.dbInt64:
                    sqlDataType = MySqlDbType.Int64;
                    break;

                case DbTypeSupported.dbDouble:
                    sqlDataType = MySqlDbType.Double;
                    break;

                case DbTypeSupported.dbDateTime:
                    sqlDataType = MySqlDbType.DateTime;
                    break;

                case DbTypeSupported.dbBlob:
                    sqlDataType = MySqlDbType.Blob;
                    break;

                case DbTypeSupported.dbDecimal:
                    sqlDataType = MySqlDbType.Decimal;
                    break;

                case DbTypeSupported.dbBit:
                    sqlDataType = MySqlDbType.Bool;
                    break;

                case DbTypeSupported.dbGUID:
                    sqlDataType = MySqlDbType.Guid;
                    break;

                default:
                    throw new ApplicationException($"Data type, [{returnType}], not supported.");
            }

            MySqlParameter returnParam = CreateDbServerParam(parameterName, sqlDataType);
            returnParam.Direction = ParameterDirection.Output;
            return returnParam;
        }

        /// <summary>
        /// Creates parameters for the supported database.
        /// </summary>
        /// <param name="obj">The Business Entity from which to extract the data</param>
        /// <param name="col">The column for which the data must be extracted from the buisiness entity</param>
        /// <param name="store">The store that handles the IO</param>
        /// <returns></returns>
        public override IDataParameter CreateParameter(object obj, TableColumn col, IDBStore store)
        {
            object extractedData = store.Extract(obj, col.Name);

            bool isNull;
            int sqlDataType;
            try
            {
                switch (col.Type)
                {
                    case DbTypeSupported.dbChar:
                    case DbTypeSupported.dbVarChar:
                    case DbTypeSupported.dbNVarChar:
                        isNull = (col.IsNullableType && extractedData == null) || extractedData == null || (string) extractedData == "";
                        sqlDataType = (int) MySqlDbType.VarChar;
                        break;

                    case DbTypeSupported.dbTinyInt:
                    case DbTypeSupported.dbInt16:
                        isNull = (col.IsNullableType && extractedData == null) || (Int16) extractedData == Int16.MinValue;
                        sqlDataType = (int) MySqlDbType.Int16;
                        break;

                    case DbTypeSupported.dbInt32:
                        isNull = (col.IsNullableType && extractedData == null) || (Int32) extractedData == Int32.MinValue;
                        sqlDataType = (int) MySqlDbType.Int32;
                        break;

                    case DbTypeSupported.dbInt64:
                        isNull = (col.IsNullableType && extractedData == null) || (Int64) extractedData == Int64.MinValue;
                        sqlDataType = (int) MySqlDbType.Int64;
                        break;

                    case DbTypeSupported.dbDouble:
                        isNull = (col.IsNullableType && extractedData == null) || (double) extractedData == double.MinValue;
                        sqlDataType = (int) MySqlDbType.Double;
                        break;

                    case DbTypeSupported.dbDateTime:
                        isNull = (col.IsNullableType && extractedData == null) || (DateTime) extractedData == DateTime.MinValue;
                        sqlDataType = (int) MySqlDbType.DateTime;
                        break;

                    case DbTypeSupported.dbBlob:    // Text, not Image
                    case DbTypeSupported.dbVarBinary:
                        isNull = extractedData == null;
                        sqlDataType = (int) MySqlDbType.VarBinary;
                        break;

                    case DbTypeSupported.dbDecimal:
                        isNull = (col.IsNullableType && extractedData == null) || (decimal) extractedData == decimal.MinValue;
                        sqlDataType = (int) MySqlDbType.Decimal;
                        break;

                    case DbTypeSupported.dbBit:
                        isNull = (col.IsNullableType && extractedData == null) || extractedData == null;
                        sqlDataType = (int) MySqlDbType.Bool;
                        break;

                    case DbTypeSupported.dbGUID:
                        isNull = (col.IsNullableType && extractedData == null) || (Guid) extractedData == Guid.Empty;
                        sqlDataType = (int) MySqlDbType.Guid;
                        if (col.IsPrimary && isNull)
                            extractedData = Guid.NewGuid().ToString();
                        break;

                    default:
                        throw new CodedThoughtApplicationException($"Data type, {col.Type}, not supported.");
                }
            }
            catch (Exception ex)
            {
                throw new CodedThoughtApplicationException("Error creating Parameter", ex);
            }

            MySqlParameter parameter = CreateDbServerParam(col.Name, (MySqlDbType) sqlDataType);
            parameter.Value = isNull ? DBNull.Value : extractedData;
            return parameter;
        }

        /// <summary>
        /// Creates and returns a return parameter for the supported database.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="returnType"></param>
        /// <returns></returns>
        /// <exception cref="ApplicationException">Data type not supported.  DataTypes currently suported are: DbTypeSupported.dbString, DbTypeSupported.dbInt32, DbTypeSupported.dbDouble, DbTypeSupported.dbDateTime, DbTypeSupported.dbChar</exception>
        public override IDataParameter CreateReturnParameter(string parameterName, DbTypeSupported returnType)
        {
            MySqlDbType sqlDataType;
            switch (returnType)
            {
                case DbTypeSupported.dbVarChar:
                case DbTypeSupported.dbChar:
                case DbTypeSupported.dbNVarChar:
                    sqlDataType = MySqlDbType.VarChar;
                    break;

                case DbTypeSupported.dbTinyInt:
                case DbTypeSupported.dbInt16:
                    sqlDataType = MySqlDbType.Int16;
                    break;

                case DbTypeSupported.dbInt32:
                    sqlDataType = MySqlDbType.Int32;
                    break;

                case DbTypeSupported.dbInt64:
                    sqlDataType = MySqlDbType.Int64;
                    break;

                case DbTypeSupported.dbDouble:
                    sqlDataType = MySqlDbType.Double;
                    break;

                case DbTypeSupported.dbDateTime:
                    sqlDataType = MySqlDbType.DateTime;
                    break;

                case DbTypeSupported.dbBlob:
                    sqlDataType = MySqlDbType.Blob;
                    break;

                case DbTypeSupported.dbDecimal:
                    sqlDataType = MySqlDbType.Decimal;
                    break;

                case DbTypeSupported.dbBit:
                    sqlDataType = MySqlDbType.Bool;
                    break;

                case DbTypeSupported.dbGUID:
                    sqlDataType = MySqlDbType.Guid;
                    break;

                default:
                    throw new ApplicationException($"Data type, [{returnType}], not supported.");
            }

            MySqlParameter returnParam = CreateDbServerParam(parameterName, sqlDataType);
            returnParam.Direction = ParameterDirection.ReturnValue;
            return returnParam;
        }

        /// <summary>Creates and returns a string parameter for the supported database.</summary>
        /// <param name="srcTableColumnName"></param>
        /// <param name="parameterValue">    </param>
        /// <returns></returns>
        public override IDataParameter CreateStringParameter(string srcTableColumnName, string parameterValue)
        {
            IDataParameter returnValue = CreateDbServerParam(srcTableColumnName, MySqlDbType.VarChar);
            returnValue.Value = parameterValue != string.Empty ? parameterValue : DBNull.Value;

            return returnValue;
        }

        /// <summary>Creates the XML parameter.</summary>
        /// <param name="srcTaleColumnName">Name of the SRC tale column.</param>
        /// <param name="parameterValue">   The parameter value.</param>
        /// <returns></returns>
        public override IDataParameter CreateXMLParameter(string srcTaleColumnName, string parameterValue) => throw new NotImplementedException();

        #endregion Parameters

        #region Add method

        /// <summary>
        /// Inserts a new record into the specified database table.
        /// </summary>
        /// <remarks>This method constructs an SQL INSERT statement dynamically based on the provided
        /// table name, schema name, and columns. If the table has an identity column, the method retrieves the
        /// generated identity value and assigns it to the primary key property of the object.</remarks>
        /// <param name="tableName">The name of the table where the record will be inserted.</param>
        /// <param name="schemaName">The schema name of the table. If null or empty, the default schema is used.</param>
        /// <param name="obj">The object containing the data to be inserted. The object's properties should match the table's columns.</param>
        /// <param name="columns">A list of <see cref="TableColumn"/> objects representing the columns of the table. Only columns marked as
        /// insertable will be included in the operation.</param>
        /// <param name="store">An instance of <see cref="IDBStore"/> used to manage database operations and set the primary key for the
        /// inserted object, if applicable.</param>
        /// <exception cref="FolderException">Thrown if a duplicate key violation occurs during the insert operation.</exception>
        /// <exception cref="ApplicationException">Thrown if the operation fails for any other reason. The exception message includes details about the table
        /// name and the underlying error.</exception>
        public override void Add(string tableName, string schemaName, object obj, List<TableColumn> columns, IDBStore store) {
            try {
                ParameterCollection parameters = [];
                StringBuilder sbColumns = new();
                StringBuilder sbValues = new();
                TableColumn? keyColumn = null;

                for (int i = 0; i < columns.Count; i++) {
                    TableColumn col = columns[i];
                    if (col.IsPrimary)
                        keyColumn = col;

                    if (col.IsInsertable) {
                        //we do not insert columns such as autonumber columns
                        IDataParameter parameter = CreateParameter(obj, col, store);
                        sbColumns.Append(__comma).Append(col.Name);
                        sbValues.Append(__comma).Append(ParameterConnector).Append(parameter.ParameterName);
                        parameters.Add(parameter);
                    }
                }

                StringBuilder sql = new($"INSERT INTO {GetTableName(schemaName, tableName)} (");
                sql.Append(sbColumns.Remove(0, 2));
                sql.Append(") VALUES (");
                sql.Append(sbValues.Remove(0, 2));
                sql.Append("); ");

                // ================================================================
                // print sql to output window to debuging purpose
                DebugParameters(sql, tableName, parameters);
                // ================================================================

                //Check if we have an identity Column
                if (store.HasKeyColumn(obj)) {
                    // Check if we have an auto numbering/identity column
                    if (keyColumn.IsIdentity) {
                        sql.Append("SELECT LAST_INSERT_ID(); ");
                        // ExecuteScalar will execute both the INSERT statement and the SELECT statement.
                        int retVal = Convert.ToInt32(ExecuteScalar(sql.ToString(), CommandType.Text, parameters));
                        store.SetPrimaryKey(obj, retVal);
                    } else {
                        ExecuteNonQuery(sql.ToString(), CommandType.Text, parameters);
                    }
                } else {
                    ExecuteNonQuery(sql.ToString(), CommandType.Text, parameters);
                }

                // this is the way to get the CONTEXT_INFO of a SQL connection session
                // string contextInfo = System.Convert.ToString( this.ExecuteScalar( "SELECT dbo.AUDIT_LOG_GET_USER_NAME() ", System.Data.CommandType.Text, null ) );
            }
            catch (ApplicationException irEx) {
                RollbackTransaction();
                // this is not a good method to catch DUPLICATE
                if (irEx.Message.IndexOf("duplicate key") >= 0) {
                    throw new FolderException(irEx.Message, irEx);
                } else {
                    throw new ApplicationException("Failed to add record to: " + tableName + "<BR>" + irEx.Message + "<BR>" + irEx.Source, irEx);
                }
            }
            catch (Exception ex) {
                RollbackTransaction();
                throw new ApplicationException("Failed to add record to: " + tableName + "<BR>" + ex.Message + "<BR>" + ex.Source, ex);
            }
        }

        /// <summary>
        /// Adds data to the database
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="obj"></param>
        /// <param name="columns"></param>
        /// <param name="store"></param>
        /// <returns></returns>
        public override void Add(string tableName, object obj, List<TableColumn> columns, IDBStore store) => Add(tableName, GetSchemaName(), obj, columns, store);

        #endregion Add method

        #region Get method

        /// <summary>Gets a data reader based on table name, columns names etc.</summary>
        /// <param name="tableName">     </param>
        /// <param name="selectColumns"> </param>
        /// <param name="parameters">    </param>
        /// <param name="orderByColumns"></param>
        /// <returns></returns>
        public override IDataReader Get(string tableName, string schemaName, List<string> selectColumns, ParameterCollection parameters, List<string> orderByColumns)
        {
            IDataReader reader = null;
            try
            {
                StringBuilder sql = new("SELECT ");
                sql.Append(GenerateColumnList(selectColumns));
                sql.Append($" FROM {GetTableName(schemaName, tableName)}");
                if (parameters != null && parameters.Count > 0)
                {
                    sql.Append(" WHERE " + GenerateWhereClauseFromParams(parameters));
                }

                sql.Append(GenerateOrderByClause(orderByColumns));
                reader = ExecuteReader(sql.ToString(), parameters);
            }
            catch (Exception ex)
            {
                throw new Exceptions.CodedThoughtApplicationException("Failed to add retrieve data from: " + tableName, ex);
            }
            finally
            {
                CommitTransaction();
            }

            return reader;
        }

        #endregion Get method

        #region Executing Queries

        //public override DataSet GetDataSet(string tableName, string schemaName, List<string> selectColumns, ParameterCollection parameters)
        //{
        //    DataSet dataSet;
        //    DefaultSchemaName = schemaName;
        //    try
        //    {
        //        StringBuilder sql = new("SELECT ");
        //        sql.Append(GenerateColumnList(selectColumns));
        //        if (DefaultSchemaName != string.Empty)
        //        {
        //            sql.AppendFormat(" FROM {0}.{1}", DefaultSchemaName, tableName);
        //        }
        //        else
        //        {
        //            sql.AppendFormat(" FROM {0}", tableName);
        //        }

        //        if (parameters != null && parameters.Count > 0)
        //        {
        //            sql.Append(" WHERE " + GenerateWhereClauseFromParams(parameters));
        //        }

        //        dataSet = ExecuteDataSet(sql.ToString(), parameters);
        //    }
        //    catch (Exception ex)
        //    {
        //        throw new Exceptions.CodedThoughtApplicationException("Failed to retrieve data from: " + tableName, ex);
        //    }

        //    return dataSet;
        //}

        /// <summary>Executes a command in the database which returns data as a DataSet</summary>
        /// <param name="commandText"></param>
        /// <param name="type">       </param>
        /// <param name="parameters"> </param>
        /// <returns></returns>
        //public override DataSet ExecuteDataSet(string commandText, CommandType type, ParameterCollection parameters)
        //{
        //    try
        //    {
        //        if (parameters != null)
        //        {
        //            if (type == CommandType.Text && parameters.Count > 0 && !commandText.ToUpper().Contains("WHERE"))
        //            {
        //                commandText += $" WHERE {GenerateWhereClauseFromParams(parameters)}";
        //            }
        //        }
        //        BeginTransaction();
        //        IDbCommand cmd = Connection.CreateCommand();
        //        cmd.CommandText = commandText;
        //        cmd.CommandType = type;
        //        cmd.CommandTimeout = CommandTimeout > -1 ? CommandTimeout : Connection.ConnectionTimeout;
        //        AddParametersToCommand(parameters, cmd);

        //        IDataAdapter adapter = CreateDataAdapter(cmd);

        //        DataSet dataSet = new();
        //        adapter.Fill(dataSet);
        //        CommitTransaction();
        //        ExtractAndReloadParameterCollection(parameters, cmd);

        //        return dataSet;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw new Exceptions.CodedThoughtApplicationException(ex.Message + "[" + commandText + "]", ex);
        //    }
        //}

        #endregion Executing Queries

        #region GetValue Methods

        /// <summary>
        /// Get a BLOB from a TEXT or IMAGE column.
        /// In order to get BLOB, a IDataReader's CommandBehavior must be set to SequentialAccess.
        /// That also means to Get columns in sequence is extremely important.
        /// Otherwise the GetBlobValue method won't return correct data.
        /// [EXAMPLE]
        /// this.DataReaderBehavior = CommandBehavior.SequentialAccess;
        ///	using(IDataReader reader = this.ExecuteReader("select BigName, ID, BigBlob from BigTable", CommandType.Text))
        ///	{
        ///		while (reader.Read())
        ///		{
        ///			string bigName = reader.GetString(0);
        ///			int id = this.GetInt32Value( reader, 1);
        ///			byte[] bigText = this.GetBlobValue( reader, 2 );
        ///		}
        ///	}
        /// </summary>
        /// <param name="reader"></param>
        ///<param name="columnName"></param>
        /// <returns></returns>
        protected override byte[] GetBlobValue(IDataReader reader, string columnName)
        {
            int position = reader.GetOrdinal(columnName);

            // The DataReader's CommandBehavior must be CommandBehavior.SequentialAccess.
            if (DataReaderBehavior != CommandBehavior.SequentialAccess)
            {
                throw new ApplicationException("Please set the DataReaderBehavior to SequentialAccess to call this method.");
            }
            MySqlDataReader sqlReader = (MySqlDataReader) reader;
            int bufferSize = 100;                   // Size of the BLOB buffer.
            byte[] outBuff = new byte[bufferSize];  // a buffer for every read in "bufferSize" bytes
            long totalBytes;                        // The total chars returned from GetBytes.
            long retval;                            // The bytes returned from GetBytes.
            long startIndex = 0;                    // The starting position in the BLOB output.
            byte[] outBytes = null;                 // The BLOB byte[] buffer holder.

            // Read the total bytes into outbyte[] and retain the number of chars returned.
            totalBytes = sqlReader.GetBytes(position, startIndex, outBytes, 0, bufferSize);
            outBytes = new byte[totalBytes];

            // initial reading from the BLOB column
            retval = sqlReader.GetBytes(position, startIndex, outBytes, 0, bufferSize);

            // Continue reading and writing while there are bytes beyond the size of the buffer.
            while (retval == bufferSize)
            {
                // Reposition the start index to the end of the last buffer and fill the buffer.
                startIndex += bufferSize;
                retval = sqlReader.GetBytes(position, startIndex, outBytes, Convert.ToInt32(startIndex), bufferSize);
            }

            return outBytes;
        }

        /// <summary>
        /// Gets a string from a BLOB, Text (SQLServer) or CLOB (Oracle),. developers should use
        /// this method only if they know for certain that the data stored in the field is a string.
        /// </summary>
        /// <param name="reader"></param>
        ///<param name="columnName"></param>
        /// <returns></returns>
        public override string GetStringFromBlob(IDataReader reader, string columnName) => Encoding.ASCII.GetString(GetBlobValue(reader, columnName));

        #endregion GetValue Methods

        #region Database Specific

        public override string ConnectionName => base.ConnectionName;

        public override DBSupported SupportedDatabase => DBSupported.MySql;

        #region Schema Definition Queries

        /// <summary>
        /// Gets the current session default schema name.
        /// </summary>
        /// <returns></returns>
        public override String GetDefaultSessionSchemaNameQuery() => throw new NotImplementedException();

        /// <summary>
        /// Gets the table's column definition query.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns><see cref="System.String"/></returns>
        public override String GetTableDefinitionQuery(string tableName)
        {
            try
            {
                StringBuilder sql = new();
                List<TableColumn> tableDefinition = [];
                // Remove any brackets since the definitiion query doesn't support that.
                string tName = tableName.Replace("[", "").Replace("]", "");
                string schemaName = GetSchemaName().Replace("[", "").Replace("]", "");
                if (tName.Split(".".ToCharArray()).Length > 1)
                {
                    // The schema name appears to have been passed along with the table name. So parse them out and use them instead of the default values.
                    string[] tableNameData = tName.Split(".".ToCharArray());
                    schemaName = tableNameData[0].Replace("[", "").Replace("]", "");
                    tName = tableNameData[1].Replace("[", "").Replace("]", "");
                }
                sql.Append("SELECT COLUMN_NAME, DATA_TYPE, ");
                sql.Append("CASE WHEN C.IS_NULLABLE = 'NO' THEN 0 ELSE 1 END as IS_NULLABLE, ");
                sql.Append("CASE WHEN C.CHARACTER_MAXIMUM_LENGTH IS NULL THEN 0 ELSE C.CHARACTER_MAXIMUM_LENGTH END AS CHARACTER_MAXIMUM_LENGTH, ");
                sql.Append("C.ORDINAL_POSITION - 1 as ORDINAL_POSITION, ");
                sql.Append("CASE WHEN C.Extra LIKE '%auto_increment%' THEN 1 ELSE 0 END as IS_IDENTITY, ");
                sql.Append("CASE WHEN C.COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END as IS_PRIMARY_KEY ");
                sql.Append("FROM INFORMATION_SCHEMA.COLUMNS C ");
                sql.Append($"WHERE TABLE_SCHEMA = '{Connection.Database}' AND TABLE_NAME = '{tName}' ");
                sql.Append("ORDER BY C.ORDINAL_POSITION");
                return sql.ToString();
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Gets the query used to list all tables in the database.
        /// </summary>
        /// <returns></returns>
        public override string GetTableListQuery() => $"SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{Connection.Database}' ORDER BY TABLE_NAME";

        /// <summary>
        /// Gets the query necessary to get a view's high level schema.  This does not include the columns.
        /// </summary>
        /// <param name="viewName"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override string GetViewDefinitionQuery(string viewName)
        {
            try
            {
                return GetTableDefinitionQuery(viewName);
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Gets the query used to list all views in the database.
        /// </summary>
        /// <returns></returns>
        public override string GetViewListQuery() => $"SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{Connection.Database}' ORDER BY TABLE_NAME";

        #endregion Schema Definition Queries

        #region Schema Methods

        /// <summary>
        /// Gets an enumerable list of <see cref="TableColumn"/> objects for the passed table.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override List<TableColumn> GetTableDefinition(string tableName)
        {
            try
            {
                List<TableColumn> tableDefinitions = [];

                DataTable dtColumns = ExecuteDataTable(GetTableDefinitionQuery(tableName));
                foreach (DataRow row in dtColumns.Rows)
                {
                    TableColumn column = new("", DbTypeSupported.dbVarChar, 0, true)
                    {
                        Name = row["COLUMN_NAME"].ToString(),
                        IsNullable = Convert.ToBoolean(row["IS_NULLABLE"]),
                        SystemType = ToSystemType(row["DATA_TYPE"].ToString()),
                        Type = ToDbSupportedType(row["DATA_TYPE"].ToString()),
                        MaxLength = Convert.ToInt64(row["CHARACTER_MAXIMUM_LENGTH"]),
                        IsIdentity = Convert.ToBoolean(row["IS_IDENTITY"]),
                        IsPrimary = Convert.ToBoolean(row["IS_PRIMARY_KEY"]),
                        OrdinalPosition = Convert.ToInt32(row["ORDINAL_POSITION"])
                    };
                    column.DbType = ToDbType(column.SystemType);
                    tableDefinitions.Add(column);
                }
                return tableDefinitions;
            }
            catch { throw; }
        }

        /// <summary>
        /// Gets an enumerable list of <see cref="TableSchema"/> objects unless tableName is passed to filter it.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override List<TableSchema> GetTableDefinitions()
        {
            try
            {
                List<TableSchema> tableDefinitions = [];
                DataTable dtTables = ExecuteDataTable(GetTableListQuery());
                foreach (DataRow row in dtTables.Rows)
                {
                    TableSchema tableSchema = new()
                    {
                        Name = row["TABLE_NAME"].ToString(),
                        Owner = row["TABLE_SCHEMA"].ToString(),
                        Columns = []
                    };
                    DataTable dtColumns = ExecuteDataTable(GetTableDefinitionQuery(tableSchema.Name));
                    foreach (DataRow col in dtColumns.Rows)
                    {
                        TableColumn column = new("", DbTypeSupported.dbVarChar, 0, true)
                        {
                            Name = col["COLUMN_NAME"].ToString(),
                            IsNullable = Convert.ToBoolean(col["IS_NULLABLE"]),
                            SystemType = ToSystemType(col["DATA_TYPE"].ToString()),
                            Type = ToDbSupportedType(col["DATA_TYPE"].ToString()),
                            MaxLength = Convert.ToInt64(col["CHARACTER_MAXIMUM_LENGTH"]),
                            IsIdentity = Convert.ToBoolean(col["IS_IDENTITY"]),
                            IsPrimary = Convert.ToBoolean(col["IS_PRIMARY_KEY"]),
                            OrdinalPosition = Convert.ToInt32(col["ORDINAL_POSITION"])
                        };
                        column.DbType = ToDbType(column.SystemType);
                        // Add this column to the list.
                        tableSchema.Columns.Add(column);
                    }
                    tableDefinitions.Add(tableSchema);
                }

                return tableDefinitions;
            }
            catch { throw; }
        }

        /// <summary>
        /// Gets an enumerable list of <see cref="TableColumn"/> objects for the passed view.
        /// </summary>
        /// <param name="viewName"></param>
        /// <returns></returns>
        public override List<TableColumn> GetViewDefinition(string viewName)
        {
            try
            {
                return GetTableDefinition(viewName);
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Gets an enumerable list of <see cref="ViewSchema"/> objects unless viewName is passed to filter it.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override List<ViewSchema> GetViewDefinitions()
        {
            try
            {
                List<ViewSchema> viewDefinitions = [];
                DataTable dtTables = ExecuteDataTable(GetViewListQuery());
                foreach (DataRow row in dtTables.Rows)
                {
                    ViewSchema viewSchema = new()
                    {
                        Name = row["TABLE_NAME"].ToString(),
                        Owner = row["TABLE_SCHEMA"].ToString(),
                        Columns = []
                    };
                    DataTable dtColumns = ExecuteDataTable(GetViewDefinitionQuery(viewSchema.Name));
                    foreach (DataRow col in dtColumns.Rows)
                    {
                        TableColumn column = new("", DbTypeSupported.dbVarChar, 0, true)
                        {
                            Name = col["COLUMN_NAME"].ToString(),
                            IsNullable = Convert.ToBoolean(col["IS_NULLABLE"]),
                            SystemType = ToSystemType(col["DATA_TYPE"].ToString()),
                            Type = ToDbSupportedType(col["DATA_TYPE"].ToString()),
                            MaxLength = Convert.ToInt64(col["CHARACTER_MAXIMUM_LENGTH"]),
                            IsIdentity = Convert.ToBoolean(col["IS_IDENTITY"]),
                            IsPrimary = Convert.ToBoolean(col["IS_PRIMARY_KEY"]),
                            OrdinalPosition = Convert.ToInt32(col["ORDINAL_POSITION"])
                        };
                        // Add this column to the list.
                        viewSchema.Columns.Add(column);
                    }
                    viewDefinitions.Add(viewSchema);
                }

                return viewDefinitions;
            }
            catch (Exception)
            {
                throw;
            }
        }

        #endregion Schema Methods

        /// <summary>
        /// Get CASE (SQL Server) or DECODE (Oracle) SQL syntax.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="equalValue"></param>
        /// <param name="trueValue"></param>
        /// <param name="falseValue"></param>
        /// <param name="alias"></param>
        /// <returns></returns>
        public override string GetCaseDecode(string columnName, string equalValue, string trueValue, string falseValue, string alias)
        {
            StringBuilder sb = new();

            sb.Append(" CASE ").Append(columnName);
            sb.Append(" WHEN ").Append(equalValue);
            sb.Append(" THEN ").Append(trueValue).Append(" ELSE ").Append(falseValue).Append(" END ");
            sb.Append(alias).Append(" ");

            return sb.ToString();
        }

        /// <summary>
        /// Get a function name that return current date
        /// </summary>
        /// <returns></returns>
        public override string GetCurrentDateFunction() => "Now()";

        /// <summary>
        /// Get a database specific date only SQL syntax.
        /// </summary>
        /// <param name="dateColumn"></param>
        /// <returns></returns>
        public override string GetDateOnlySqlSyntax(string dateColumn) => "CSTR(" + dateColumn + ")";

        /// <summary>
        /// Gets  date part(Day, month or year) of date
        /// </summary>
        /// <param name="datestring"></param>
        /// <param name="dateFormat"></param>
        /// <param name="datePart"></param>
        /// <returns></returns>
        public override string GetDatePart(string datestring, DateFormat dateFormat, DatePart datePart)
        {
            string datePartstring = string.Empty;
            switch (datePart)
            {
                case DatePart.DAY:
                    datePartstring = $"DAY({datestring})";
                    break;

                case DatePart.MONTH:
                    datePartstring = $"MONTH({datestring})";
                    break;

                case DatePart.YEAR:
                    datePartstring = $"YEAR({datestring})";
                    break;
            }
            return datePartstring;
        }

        /// <summary>
        /// Gets Date string format.
        /// </summary>
        /// <param name="columnName">Name of the column.</param>
        /// <param name="dateFormat">The date format.</param>
        /// <returns></returns>
        public override string GetDateToStringForColumn(string columnName, DateFormat dateFormat)
        {
            StringBuilder sb = new StringBuilder();
            switch (dateFormat)
            {
                case DateFormat.MMDDYYYY:
                    sb.Append(" FORMAT(").Append(columnName).Append(", \"mm/dd/yyyy\") ");
                    break;

                case DateFormat.MMDDYYYY_Hyphen:
                    sb.Append(" FORMAT(").Append(columnName).Append(", \"mm-dd-yyyy\") ");
                    break;

                case DateFormat.MonDDYYYY:
                    sb.Append(" FORMAT(").Append(columnName).Append(", \"mmm d yyyy\") ");
                    break;

                default:
                    sb.Append(columnName);
                    break;
            }
            return sb.ToString();
        }

        /// <summary>
        /// Gets the date to string for value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="dateFormat">The date format.</param>
        /// <returns></returns>
        public override string GetDateToStringForValue(string value, DateFormat dateFormat)
        {
            StringBuilder sb = new();
            switch (dateFormat)
            {
                case DateFormat.MMDDYYYY:
                    sb.Append(" FORMAT(\"").Append(value).Append("\", \"mm/dd/yyyy\") ");
                    break;

                case DateFormat.MMDDYYYY_Hyphen:
                    sb.Append(" FORMAT(\"").Append(value).Append("\", \"mm-dd-yyyy\") ");
                    break;

                case DateFormat.MonDDYYYY:
                    sb.Append(" FORMAT(\"").Append(value).Append("\", \"mmm d yyyy\") ");
                    break;

                default:
                    sb.Append(value);
                    break;
            }
            return sb.ToString();
        }

        /// <summary>
        /// Gets database function name
        /// </summary>
        /// <param name="functionName"></param>
        /// <returns></returns>
        public override string GetFunctionName(FunctionName functionName)
        {
            string retStr = string.Empty;
            switch (functionName)
            {
                case FunctionName.SUBSTRING:
                    retStr = "LEFT";
                    break;

                case FunctionName.ISNULL:
                    retStr = "ISNULL";
                    break;

                case FunctionName.CURRENTDATE:
                    retStr = "NOW()";
                    break;

                case FunctionName.CONCATENATE:
                    retStr = "&";
                    break;
            }
            return retStr;
        }

        /// <summary>
        /// Get an IsNull (SQLServer) or NVL (Oracle)
        /// </summary>
        /// <param name="validateColumnName"></param>
        /// <param name="optionColumnName"></param>
        /// <returns></returns>
        public override string GetIfNullFunction(string validateColumnName, string optionColumnName) => $" {GetIfNullFunction()}(" + validateColumnName + ", " + optionColumnName + ") ";

        /// <summary>
        /// Get a function name for NULL validation
        /// </summary>
        /// <returns></returns>
        public override string GetIfNullFunction() => "isnull";

        public override string GetSchemaName() => !String.IsNullOrEmpty(DefaultSchemaName) ? DefaultSchemaName : String.Empty;

        /// <summary>
        /// Get a database specific syntax that converts string to date.
        /// Oracle does not convert date string to date implicitly like SQL Server does
        /// when there is a date comparison.
        /// </summary>
        /// <param name="dateString"></param>
        /// <returns></returns>
        public override string GetStringToDateSqlSyntax(string dateString) => "#" + dateString + "# ";

        /// <summary>
        /// Get a database specific syntax that converts string to date.
        /// Oracle does not convert date string to date implicitly like SQL Server does
        /// when there is a date comparison.
        /// </summary>
        /// <param name="dateSQL"></param>
        /// <returns></returns>
        public override string GetStringToDateSqlSyntax(DateTime dateSQL) => "#" + dateSQL.ToString("G", System.Globalization.DateTimeFormatInfo.InvariantInfo) + "# ";

        public override string GetTableName(string defaultSchema, string tableName)
        {
            if (!String.IsNullOrEmpty(defaultSchema))
            {
                return $"{defaultSchema}.{tableName}";
            }
            else
            {
                if(!string.IsNullOrEmpty(GetSchemaName())) {
                    return $"{GetSchemaName()}.{tableName}";
                }
                return tableName;
            }
        }

        /// <summary>
        /// Gets SQL syntax of Year
        /// </summary>
        /// <param name="dateString"></param>
        /// <returns></returns>
        public override string GetYearSQLSyntax(string dateString) => "FORMAT(#" + dateString + "#, \"yyyy\")";

        /// <summary>
        /// Convert a datestring to datetime when used for between.... and
        /// </summary>
        /// <param name="datestring">string</param>
        /// <param name="dateFormat">DatabaseObject.DateFormat</param>
        /// <returns></returns>
        public override string ToDate(string datestring, DateFormat dateFormat) => __singleQuote + datestring + __singleQuote;

        /// <summary>
        /// Converts a database type name to a <see cref="DbTypeSupported"/> type.
        /// </summary>
        /// <param name="dbTypeName"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override DbTypeSupported ToDbSupportedType(string dbTypeName)
        {
            Dictionary<string, DbTypeSupported> typeMap = new Dictionary<string, DbTypeSupported>()
            {
                { "char", DbTypeSupported.dbChar },
                { "varchar", DbTypeSupported.dbNVarChar },
                { "text", DbTypeSupported.dbNVarChar },
                { "longtext", DbTypeSupported.dbNVarChar },
                { "mediumtext", DbTypeSupported.dbNVarChar },
                { "blob", DbTypeSupported.dbBlob },
                { "longblob", DbTypeSupported.dbImage },
                { "mediumblob", DbTypeSupported.dbImage },
                { "int", DbTypeSupported.dbInt32 },
                { "smallint", DbTypeSupported.dbInt16 },
                { "tinyint", DbTypeSupported.dbTinyInt },
                { "bit", DbTypeSupported.dbBit },
                { "mediumint", DbTypeSupported.dbInt32 }, // Mapping to int in SQL Server
                { "bigint", DbTypeSupported.dbInt64 },
                { "float", DbTypeSupported.dbDouble },
                { "double", DbTypeSupported.dbDouble },
                { "decimal", DbTypeSupported.dbDecimal },
                { "date", DbTypeSupported.dbDateTime },
                { "datetime", DbTypeSupported.dbDateTime },
                { "timestamp", DbTypeSupported.dbDateTime2 }, // Mapping to datetime in SQL Server
                { "time", DbTypeSupported.dbTime },
                { "year", DbTypeSupported.dbInt16 }, // Mapping to smallint in SQL Server
                { "guid", DbTypeSupported.dbGUID },
            };

            return typeMap.ContainsKey(dbTypeName.ToLower())
                ? typeMap[dbTypeName.ToLower()]
                : throw new ArgumentException($"MySQL data type, {dbTypeName}, not a supported or recognized DbTypeSupported type.");
        }

        /// <summary>
        /// Converts a database type name to a system type.
        /// </summary>
        /// <param name="dbTypeName">Name of the db type.</param>
        /// <returns>
        /// System.Type
        /// </returns>
        /// <exception cref="System.NotImplementedException"></exception>
        public override Type ToSystemType(string mySqlType)
        {
            Dictionary<string, Type> typeMap = new Dictionary<string, Type>()
            {
                { "char", typeof(string) },
                { "varchar", typeof(string) },
                { "text", typeof(string) },
                { "blob", typeof(byte[]) },
                { "longblob", typeof(byte[]) },
                { "mediumblob", typeof(byte[]) },
                { "longtext", typeof(string) },
                { "mediumtext", typeof(string) },
                { "int", typeof(int) },
                { "tinyint", typeof(byte) },
                { "smallint", typeof(short) },
                { "mediumint", typeof(int) }, // Mapping to int in SQL Server
                { "bigint", typeof(long) },
                { "float", typeof(double) },
                { "double", typeof(double) },
                { "decimal", typeof(decimal) },
                { "bit", typeof(Boolean) },
                { "date", typeof(DateTime) },
                { "datetime", typeof(DateTime) },
                { "timestamp", typeof(DateTime) }, // Mapping to datetime in SQL Server
                { "time", typeof(TimeSpan) },
                { "year", typeof(short) }, // Mapping to smallint in SQL Server
                { "guid", typeof(Guid) }
                // Add more mappings as needed
            };

            if (typeMap.ContainsKey(mySqlType.ToLower()))
            {
                return typeMap[mySqlType.ToLower()];
            }
            else
            {
                throw new ArgumentException($"MySQL data type, {mySqlType}, not supported or recognized.");
            }
        }


        #endregion Database Specific

        #endregion Other Override Methods
    }
}