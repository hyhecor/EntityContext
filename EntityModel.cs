
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;

namespace Models
{
    public class EntityModel : IDisposable
    {
        #region IDisposable
        private bool disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: 관리형 상태(관리형 개체)를 삭제합니다.
                    if (null != this._baseTable) this._baseTable.Dispose();
                }

                // TODO: 비관리형 리소스(비관리형 개체)를 해제하고 종료자를 재정의합니다.
                // TODO: 큰 필드를 null로 설정합니다.
                this._baseTable = null;
                disposedValue = true;
            }
        }

        // // TODO: 비관리형 리소스를 해제하는 코드가 'Dispose(bool disposing)'에 포함된 경우에만 종료자를 재정의합니다.
        // ~EntityModel()
        // {
        //     // 이 코드를 변경하지 마세요. 'Dispose(bool disposing)' 메서드에 정리 코드를 입력합니다.
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // 이 코드를 변경하지 마세요. 'Dispose(bool disposing)' 메서드에 정리 코드를 입력합니다.
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
        #endregion

        /// <summary>
        /// BaseMsSql
        /// </summary>
        public IDbConnection DbConnection { get; set; }
        public IDbTransaction DbTransaction { get; set; }
        /// <summary>
        /// 기본 DataTable
        /// </summary>
        DataTable _baseTable { get; set; }
        /// <summary>
        /// 기본 테이블 이름
        /// </summary>
        string _baseTableName { get; set; }
        public string DataTableName { get { return this._baseTableName; } }

        public string ConnectionString { get { return this.DbConnection.ConnectionString; } }
        /// <summary>
        /// 생성자
        /// </summary>
        /// <param name="pTableName"></param>
        /// <param name="pDbConn"></param>
        public EntityModel(string pTableName, IDbConnection pDbConn)
        {
            this.DbConnection = pDbConn;
            this._baseTableName = pTableName;
        }
        public EntityModel(string pTableName, IDbConnection pDbConn, IDbTransaction pDbTran)
        {
            this.DbConnection = pDbConn;
            this.DbTransaction = pDbTran;
            this._baseTableName = pTableName;
        }
        void setDataTable(DataTable pDataTable)
        {
            pDataTable.TableName = this.DataTableName;

            this._baseTable = pDataTable;
        }
        public void SetTableRows(DataTable pDataTable)
        {
            this.GetDataTable().Rows.Clear();

            foreach (DataRow row in pDataTable.Rows)
            {
                DataRow newRow = this.GetDataTable().NewRow();
                foreach (DataColumn col in newRow.Table.Columns)
                {
                    newRow[col.ColumnName] = row[col.ColumnName];
                }
                this.GetDataTable().Rows.Add(newRow);
            }
            // 변경사항을 초기화
            this.GetDataTable().AcceptChanges();
        }

        public DataColumn[] PrimaryKey
        {
            get
            {
                return this.GetDataTable().PrimaryKey;
            }
        }
        public DataColumn[] Columns
        {
            get
            {
                List<DataColumn> dataColumns = new List<DataColumn>();
                foreach (DataColumn item in this.GetDataTable().Columns)
                {
                    dataColumns.Add(item);
                }
                return dataColumns.ToArray();
            }
        }

        /// <summary>
        /// GetDataTable
        /// </summary>
        /// <returns></returns>
        public DataTable GetDataTable()
        {
            if (null != this._baseTable)
            {
                return this._baseTable;
            }
            this._baseTable = EntityModelRegister.Clone(this.DbConnection, this.DataTableName);
            if (null != this._baseTable)
            {
                return this._baseTable;
            }

            DataTable dt = getDataTableSchema(this.DataTableName, this.DbConnection, this.DbTransaction);

            if (null == dt)
                return null;

            #region OleDbConnection.GetOleDbSchemaTable 테스트 
#if false
            if (this._DbConn is OleDbConnection)
            {
                {
                    DataTable schemaTable = ((OleDbConnection)this._DbConn).GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] 
                        {
                            null, // TABLE_CATALOG
                            null, // TABLE_SCHEMA
                            this.DataTableName, // TABLE_NAME * 
                            null, // TABLE_TYPE
                        });
                }
                {
                    DataTable schemaTable = ((OleDbConnection)this._DbConn).GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, new object[]
                        {
                            null, // TABLE_CATALOG
                            null, // TABLE_SCHEMA
                            this.DataTableName, // TABLE_NAME * 
                        });
                }
            }
#endif
            #endregion
            if (this.DbConnection is SqlConnection)
            {
                dt.PrimaryKey = getDataTablePrimaryKey(dt, this.DataTableName, this.DbConnection, this.DbTransaction);
            }
            if (this.DbConnection is OleDbConnection)
            {
                dt.PrimaryKey = getOleDataTablePrimaryKey(dt, this.DataTableName, (OleDbConnection)this.DbConnection, (OleDbTransaction)this.DbTransaction);
            }

            // 복제본을 지정
            this.setDataTable(dt.Clone());
            EntityModelRegister.Add(this.DbConnection, this.DataTableName, this._baseTable);

            return this._baseTable;

        }
        #region Insert
        /// <summary>
        /// Insert
        /// </summary>
        /// <returns></returns>
        public Int_Exception Insert()
        {

            string fmtQuery = @"
SELECT * FROM {0}
;
";
            // 인서트 레코드 선택
            foreach (DataRow row in this.GetDataTable().Rows)
                row.SetAdded();

            string query = string.Format(fmtQuery,
                this.DataTableName
                );
            //Debug.WriteLine(query);

            using (IDbCommand cmd = this.DbConnection.CreateCommand())
            {
                cmd.CommandText = query;
                cmd.CommandType = CommandType.Text;
                cmd.Transaction = this.DbTransaction;

                int nRowUpdated = 0;
                IDbDataAdapter adapter = null;
                DbCommandBuilder commandBuilder = null;
                if (this.DbConnection is SqlConnection)
                {
                    adapter = new SqlDataAdapter((SqlCommand)cmd);
                    commandBuilder = new SqlCommandBuilder((SqlDataAdapter)adapter);

                    ((SqlDataAdapter)adapter).RowUpdated += (s, e) => { ++nRowUpdated; };
                }
                if (this.DbConnection is OleDbConnection)
                {
                    adapter = new OleDbDataAdapter((OleDbCommand)cmd);
                    commandBuilder = new OleDbCommandBuilder((OleDbDataAdapter)adapter);

                    ((OleDbDataAdapter)adapter).RowUpdated += (s, e) => { ++nRowUpdated; };
                }

                try
                {
                    adapter.InsertCommand = commandBuilder.GetInsertCommand();
                    //Debug.WriteLine(adapter.InsertCommand.CommandText);
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }

                try
                {
                    if (this.DbConnection is SqlConnection)
                        ((SqlDataAdapter)adapter).Update(this.GetDataTable());
                    if (this.DbConnection is OleDbConnection)
                        ((OleDbDataAdapter)adapter).Update(this.GetDataTable());
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }

                return new Int_Exception(nRowUpdated);
            }
        }
        #endregion
        #region Update
        /// <summary>
        /// Update
        /// </summary>
        /// <param name="pKeyValue"></param>
        /// <param name="opt"></param>
        /// <returns></returns>
        public Int_Exception Update(Hashtable pKeyValue, params string[] opt)
        {
            string fmtQuery = @"
SELECT * FROM {0}
;
";
            foreach (DataRow row in this.GetDataTable().Rows)
                foreach (string key in pKeyValue.Keys)
                    row[key] = pKeyValue[key];


            string query = string.Format(fmtQuery,
                this.DataTableName
                );
            //Debug.WriteLine(query);

            using (IDbCommand cmd = this.DbConnection.CreateCommand())
            {
                cmd.CommandText = query;
                cmd.CommandType = CommandType.Text;
                cmd.Transaction = this.DbTransaction;

                int nRowUpdated = 0;
                IDbDataAdapter adapter = null;
                DbCommandBuilder commandBuilder = null;
                if (this.DbConnection is SqlConnection)
                {
                    adapter = new SqlDataAdapter((SqlCommand)cmd);
                    commandBuilder = new SqlCommandBuilder((SqlDataAdapter)adapter);

                    ((SqlDataAdapter)adapter).RowUpdated += (s, e) => { ++nRowUpdated; };
                }
                if (this.DbConnection is OleDbConnection)
                {
                    adapter = new OleDbDataAdapter((OleDbCommand)cmd);
                    commandBuilder = new OleDbCommandBuilder((OleDbDataAdapter)adapter);

                    ((OleDbDataAdapter)adapter).RowUpdated += (s, e) => { ++nRowUpdated; };
                }

                try
                {
                    adapter.UpdateCommand = commandBuilder.GetUpdateCommand();
                    //Debug.WriteLine(adapter.UpdateCommand.CommandText);
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }

                try
                {
                    if (this.DbConnection is SqlConnection)
                        ((SqlDataAdapter)adapter).Update(this.GetDataTable());
                    if (this.DbConnection is OleDbConnection)
                        ((OleDbDataAdapter)adapter).Update(this.GetDataTable());
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }

                return new Int_Exception(nRowUpdated);
            }
        }
        #endregion
        #region Delete
        /// <summary>
        /// Delete
        /// </summary>
        /// <param name="opt"></param>
        /// <returns></returns>
        public Int_Exception Delete(params string[] opt)
        {
            string fmtQuery = @"
SELECT * FROM {0}
;
";
            foreach (DataRow row in this.GetDataTable().Rows)
                row.Delete();

            string query = string.Format(fmtQuery,
                this.DataTableName
                );
            //Debug.WriteLine(query);

            using (IDbCommand cmd = this.DbConnection.CreateCommand())
            {
                cmd.CommandText = query;
                cmd.CommandType = CommandType.Text;
                cmd.Transaction = this.DbTransaction;

                int nRowUpdated = 0;
                IDbDataAdapter adapter = null;
                DbCommandBuilder commandBuilder = null;
                if (this.DbConnection is SqlConnection)
                {
                    adapter = new SqlDataAdapter((SqlCommand)cmd);
                    commandBuilder = new SqlCommandBuilder((SqlDataAdapter)adapter);

                    ((SqlDataAdapter)adapter).RowUpdated += (s, e) => { ++nRowUpdated; };
                }
                if (this.DbConnection is OleDbConnection)
                {
                    adapter = new OleDbDataAdapter((OleDbCommand)cmd);
                    commandBuilder = new OleDbCommandBuilder((OleDbDataAdapter)adapter);

                    ((OleDbDataAdapter)adapter).RowUpdated += (s, e) => { ++nRowUpdated; };
                }

                try
                {
                    adapter.DeleteCommand = commandBuilder.GetDeleteCommand();
                    //Debug.WriteLine(adapter.DeleteCommand.CommandText);
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }

                try
                {
                    if (this.DbConnection is SqlConnection)
                        ((SqlDataAdapter)adapter).Update(this.GetDataTable());
                    if (this.DbConnection is OleDbConnection)
                        ((OleDbDataAdapter)adapter).Update(this.GetDataTable());
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }

                return new Int_Exception(nRowUpdated);
            }
        }
        #endregion
        #region Select
        /// <summary>
        /// Select
        /// </summary>
        /// <param name="param"></param>
        /// <param name="opt"></param>
        /// <returns></returns>
        public Int_Exception Select(Hashtable param, params string[] opt)
        {
            string fmtQuery = @"
SELECT {2} FROM {0} WITH(NOLOCK)
        WHERE 1=1 
          {1}
{3}
;
";
            if (this.DbConnection is OleDbConnection)
            {
                fmtQuery = fmtQuery.Replace("WITH(NOLOCK)", "");
            }

            List<string> columns = new List<string>();

            foreach (DataColumn col in this.Columns)
            {
                columns.Add(string.Format("{0}",
                        col.ColumnName
                        ));
            }

            List<string> conds = new List<string>();

            foreach (object key in param.Keys)
            {
                conds.Add(string.Format("AND {0}='{1}'",
                        key,
                        param[key]
                        ));
            }
            string query = string.Format(fmtQuery,
                this.DataTableName,
                string.Join(" ", conds),
                string.Join(", ", columns),
                string.Join("\r\n", opt)
                );
            Debug.WriteLine(query);

            this.GetDataTable().Rows.Clear();
            using (IDbCommand cmd = this.DbConnection.CreateCommand())
            {
                cmd.CommandText = query;
                cmd.CommandType = CommandType.Text;
                cmd.Transaction = this.DbTransaction;

                using (IDataReader reader = cmd.ExecuteReader())
                {
                    try
                    {
                        this.GetDataTable().Load(reader);
                    }
                    catch (Exception ex)
                    {
                        return new Int_Exception(ex);
                    }

                    return new Int_Exception(this.GetDataTable().Rows.Count);
                }
            }
        }
        #endregion

        #region SqlBulkCopy
        public Int_Exception SqlBulkCopy()
        {
            if (this.DbConnection is SqlConnection)
            {
                SqlBulkCopyOptions opt = SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock;
                opt = SqlBulkCopyOptions.Default;

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy((SqlConnection)this.DbConnection, opt, (SqlTransaction)this.DbTransaction))
                {
                    bulkCopy.DestinationTableName = this.DataTableName;

                    foreach (var column in this.Columns)
                        bulkCopy.ColumnMappings.Add(column.ToString(), column.ToString());


                    int nSqlRowsCopied = 0;
                    bulkCopy.SqlRowsCopied += (s, e) => { ++nSqlRowsCopied; };

                    try
                    {
                        bulkCopy.WriteToServer(this.GetDataTable());
                    }
                    catch (Exception ex)
                    {
                        return new Int_Exception(ex);
                    }
                    return new Int_Exception(nSqlRowsCopied);
                }
            }
            else if (this.DbConnection is OleDbConnection)
            {
                string fmt_query = @"
SELECT * FROM {0} 
;
";
                string query = string.Format(fmt_query,
                    this.DataTableName
                    );

                using (OleDbCommand cmd = (OleDbCommand)this.DbConnection.CreateCommand())
                {
                    cmd.CommandText = query;
                    cmd.CommandType = CommandType.Text;
                    cmd.Transaction = (OleDbTransaction)this.DbTransaction;

                    // 인서트 레코드 선택
                    foreach (DataRow r in this.GetDataTable().Rows)
                        if (r.RowState == DataRowState.Unchanged) r.SetAdded();

                    using (OleDbDataAdapter adapter = new OleDbDataAdapter(cmd))
                    {
                        int nRowUpdated = 0;
                        adapter.RowUpdating += (s, e) => { };
                        adapter.RowUpdated += (s, e) => { ++nRowUpdated; };

                        try
                        {
                            OleDbCommandBuilder oleDbCommandBuilder = new OleDbCommandBuilder(adapter);
                            adapter.InsertCommand = oleDbCommandBuilder.GetInsertCommand();
                        }
                        catch (Exception ex)
                        {
                            return new Int_Exception(ex);
                        }

                        try
                        {
                            adapter.Update(this.GetDataTable()); // DataTable 업로드
                        }
                        catch (Exception ex)
                        {
                            return new Int_Exception(ex);
                        }

                        return new Int_Exception(nRowUpdated);
                    }
                }
            }
            else
            {
                return new Int_Exception(0);
            }
        }
        #endregion


        #region statics
        /// <summary>
        /// getDataTableSchema - 테이블 스키마 가져오기
        /// </summary>
        /// <param name="pTableName"></param>
        /// <param name="pDbConn"></param>
        /// <returns></returns>
        static DataTable getDataTableSchema(string pTableName, IDbConnection pDbConn, IDbTransaction pDbTran)
        {
            string q = string.Format("SELECT TOP 1 * FROM {0} WHERE 1=0; ", pTableName);

            using (IDbCommand cmd = pDbConn.CreateCommand())
            {
                cmd.CommandText = q;
                cmd.CommandType = CommandType.Text;
                cmd.Transaction = pDbTran;

                using (DataTable dt = new DataTable())
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    dt.Load(reader);
                    dt.TableName = pTableName;
                    return dt.Clone();
                }
            }
        }
        /// <summary>
        /// getDataTablePrimaryKey - 테이블 기본키 가져오기 
        /// </summary>
        /// <param name="pDataTable"></param>
        /// <param name="pTableName"></param>
        /// <param name="pDbConn"></param>
        /// <returns></returns>
        static DataColumn[] getDataTablePrimaryKey(DataTable pDataTable, string pTableName, IDbConnection pDbConn, IDbTransaction pDbTran)
        {
            string q = string.Format("SELECT * FROM {0} WHERE TABLE_NAME = '{1}'; ", "INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE", pTableName);

            using (IDbCommand cmd = pDbConn.CreateCommand())
            {
                cmd.CommandText = q;
                cmd.CommandType = CommandType.Text;
                cmd.Transaction = pDbTran;

                using (DataTable ConstraintColumnUsage = new DataTable())
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    ConstraintColumnUsage.Load(reader);
                    int nRet = ConstraintColumnUsage.Rows.Count;

                    List<DataColumn> PrimaryKeys = new List<DataColumn>();
                    foreach (DataRow row in ConstraintColumnUsage.Rows)
                    {
                        DataColumn pkColumn = null;
                        string columnName = row.Field<string>("COLUMN_NAME");
                        foreach (DataColumn column in pDataTable.Columns)
                        {
                            if (column.ColumnName == columnName)
                            {
                                pkColumn = column;
                                break;
                            }
                        }
                        if (null != pkColumn)
                            PrimaryKeys.Add(pkColumn);
                    }
                    return PrimaryKeys.ToArray();
                }
            }
        }
        /// <summary>
        /// getOleDataTablePrimaryKey - 테이블 기본키 가져오기 
        /// </summary>
        /// <param name="pDataTable"></param>
        /// <param name="pTableName"></param>
        /// <param name="pDbConn"></param>
        /// <returns></returns>
        static DataColumn[] getOleDataTablePrimaryKey(DataTable pDataTable, string pTableName, OleDbConnection pDbConn, OleDbTransaction pDbTran)
        {
            DataTable ConstraintColumnUsage = pDbConn.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, new object[]
            {
                null, // TABLE_CATALOG
                null, // TABLE_SCHEMA
                pTableName, // TABLE_NAME * 
            });

            List<DataColumn> PrimaryKeys = new List<DataColumn>();
            foreach (DataRow row in ConstraintColumnUsage.Rows)
            {
                DataColumn pkColumn = null;
                string columnName = row.Field<string>("COLUMN_NAME");
                foreach (DataColumn column in pDataTable.Columns)
                {
                    if (column.ColumnName == columnName)
                    {
                        pkColumn = column;
                        break;
                    }
                }
                if (null != pkColumn)
                    PrimaryKeys.Add(pkColumn);
            }
            return PrimaryKeys.ToArray();
        }
        #endregion
    }

    public class Int_Exception : Tuple<int, Exception>
    {
        public Int_Exception(int @int, Exception @exception)
            : base(@int, @exception)
        {

        }
        public Int_Exception(int @int)
            : base(@int, null)
        {

        }
        public Int_Exception(Exception @exception)
            : base(0, @exception)
        {

        }

        public int Count { get { return base.Item1; } }
        public Exception Err { get { return base.Item2; } }

        public static bool ErrorCheck(Int_Exception p)
        {
            if (null != p.Err)
            {
                foreach(Action<Exception> fn in FnErrorCheckActions)
                {
                    fn(p.Err);
                }
                return true;
            }
            return false;
        }
        public static void AddErrorCheckAction(Action<Exception> fnErrorCheckAction)
        {
            FnErrorCheckActions.Add(fnErrorCheckAction);
        }
        public static void RemoveErrorCheckAction(Action<Exception> fnErrorCheckAction)
        {
            if(FnErrorCheckActions.Contains(fnErrorCheckAction))
                FnErrorCheckActions.Remove(fnErrorCheckAction);
        }
        static List<Action<Exception>> FnErrorCheckActions = new List<Action<Exception>>();
    }
    public static class DataRowExtensions
    {
        public static T FieldOrDefault<T>(this DataRow row, string columnName)
        {
            return row.IsNull(columnName) ? default(T) : row.Field<T>(columnName);
        }
    }

#if false
    public static class ExtensionDbType
    {
        /// <summary>
        /// ToDbType [convert from 'System.Type' to 'System.Data.DbType']
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static System.Data.DbType ToDbType(this System.Type type)
        {
            if (null == mapToDbType)
                mapToDbType = new System.Collections.Generic.Dictionary<System.Type, System.Data.DbType>()
                {
                    {typeof(byte),System.Data.DbType.Byte },
                    {typeof(sbyte), System.Data.DbType.SByte },
                    {typeof(short), System.Data.DbType.Int16 },
                    {typeof(ushort), System.Data.DbType.UInt16 },
                    {typeof(int), System.Data.DbType.Int32 },
                    {typeof(uint), System.Data.DbType.UInt32 },
                    {typeof(long),System.Data.DbType.Int64 },
                    {typeof(ulong), System.Data.DbType.UInt64 },
                    {typeof(float), System.Data.DbType.Single },
                    {typeof(double), System.Data.DbType.Double },
                    {typeof(decimal), System.Data.DbType.Decimal },
                    {typeof(bool), System.Data.DbType.Boolean },
                    {typeof(string), System.Data.DbType.String },
                    {typeof(char), System.Data.DbType.StringFixedLength },
                    {typeof(System.Guid), System.Data.DbType.Guid },
                    {typeof(System.DateTime), System.Data.DbType.DateTime },
                    {typeof(System.DateTimeOffset), System.Data.DbType.DateTimeOffset },
                    {typeof(byte[]), System.Data.DbType.Binary },
                    {typeof(byte?), System.Data.DbType.Byte },
                    {typeof(sbyte?), System.Data.DbType.SByte },
                    {typeof(short?), System.Data.DbType.Int16 },
                    {typeof(ushort?), System.Data.DbType.UInt16 },
                    {typeof(int?), System.Data.DbType.Int32 },
                    {typeof(uint?), System.Data.DbType.UInt32 },
                    {typeof(long?), System.Data.DbType.Int64 },
                    {typeof(ulong?), System.Data.DbType.UInt64 },
                    {typeof(float?), System.Data.DbType.Single },
                    {typeof(double?), System.Data.DbType.Double },
                    {typeof(decimal?), System.Data.DbType.Decimal },
                    {typeof(bool?), System.Data.DbType.Boolean },
                    {typeof(char?), System.Data.DbType.StringFixedLength },
                    {typeof(System.Guid?), System.Data.DbType.Guid },
                    {typeof(System.DateTime?), System.Data.DbType.DateTime },
                    {typeof(System.DateTimeOffset?), System.Data.DbType.DateTimeOffset },
                    {typeof(System.Data.Linq.Binary), System.Data.DbType.Binary },
                };


            return mapToDbType[type];
        }
        static System.Collections.Generic.Dictionary<System.Type, System.Data.DbType> mapToDbType = null;


        public static System.Data.OleDb.OleDbType ToOleDbType(this System.Type type)
        {
            if (null == mapOleDbType)
                mapOleDbType = new Dictionary<Type, OleDbType> {
                    {typeof(string), OleDbType.VarChar },
                    {typeof(int), OleDbType.Integer },
                    {typeof(uint), OleDbType.UnsignedInt },
                    {typeof(long), OleDbType.BigInt },
                    {typeof(ulong), OleDbType.UnsignedBigInt },
                    {typeof(byte[]), OleDbType.Binary },
                    {typeof(bool), OleDbType.Boolean },
                    {typeof(decimal), OleDbType.Decimal },
                    {typeof(System.DateTime), OleDbType.Date },
                    {typeof(System.TimeSpan), OleDbType.DBTime },
                    {typeof(double), OleDbType.Double },
                    {typeof(System.Exception),OleDbType.Error },
                    {typeof(System.Guid), OleDbType.Guid },
                    {typeof(float), OleDbType.Single },
                    {typeof(short), OleDbType.SmallInt },
                    {typeof(ushort), OleDbType.UnsignedSmallInt },
                    {typeof(sbyte), OleDbType.TinyInt },
                    {typeof(byte), OleDbType.UnsignedTinyInt },
                    {typeof(char), OleDbType.Char },
                     {typeof(sbyte?), OleDbType.TinyInt },
                    {typeof(short?), OleDbType.SmallInt },
                    {typeof(ushort?), OleDbType.UnsignedSmallInt },
                    {typeof(int?), OleDbType.Integer },
                    {typeof(uint?), OleDbType.UnsignedInt },
                    {typeof(long?), OleDbType.BigInt },
                    {typeof(ulong?), OleDbType.UnsignedBigInt },
                    {typeof(float?), OleDbType.Single },
                    {typeof(double?), OleDbType.Double },
                    {typeof(decimal?), OleDbType.Decimal },
                    {typeof(bool?), OleDbType.Boolean },
                    {typeof(char?), OleDbType.Char },
                    {typeof(System.Guid?), OleDbType.Guid },
                    {typeof(System.DateTime?), OleDbType.Date },
                    {typeof(System.TimeSpan?), OleDbType.DBTime },
                };

            return mapOleDbType[type];
        }
        static System.Collections.Generic.Dictionary<System.Type, System.Data.OleDb.OleDbType> mapOleDbType = null;
    }
#endif
}

