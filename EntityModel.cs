using GPOS.FW.BaseLibrary;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Linq;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace POS.SVC.Main.EntityModels
{
    public class EntityContext : IDisposable
    {
        #region IDisposable

        internal List<IDisposable> Disposables = new List<IDisposable>();

        private bool disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: 관리형 상태(관리형 개체)를 삭제합니다.
                    if (null != this._baseTable) this._baseTable.Dispose();

                    foreach (IDisposable item in this.Disposables)
                        if (null != item) item.Dispose();
                }

                // TODO: 비관리형 리소스(비관리형 개체)를 해제하고 종료자를 재정의합니다.
                // TODO: 큰 필드를 null로 설정합니다.
                this._baseTable = null;

                this.Disposables.Clear();

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
        public EntityContext(string pTableName, IDbConnection pDbConn)
        {
            this.DbConnection = pDbConn;
            this._baseTableName = pTableName;
        }
        public EntityContext(string pTableName, IDbConnection pDbConn, IDbTransaction pDbTran)
        {
            this.DbConnection = pDbConn;
            this.DbTransaction = pDbTran;
            this._baseTableName = pTableName;
        }

        public EntityContext(string pTableName, Func<IDbConnection> pFnDbConn)
        {
            this.DbConnection = pFnDbConn();
            this.Disposables.Add(this.DbConnection);

            this._baseTableName = pTableName;
        }
        void setDataTable(DataTable pDataTable)
        {
            pDataTable.TableName = this.DataTableName;

            this._baseTable = pDataTable;
        }
        public Int_Exception SetTableRows(DataTable pDataTable)
        {
            this.GetDataTable().Rows.Clear();

            return AddOrReplaceTableRows(pDataTable);
        }
        public Int_Exception SetTableRows(DataRow[] pDataRow)
        {
            this.GetDataTable().Rows.Clear();

            return AddOrReplaceTableRows(pDataRow);
        }
        public Int_Exception AddOrReplaceTableRows(DataTable pDataTable)
        {
            Func<DataTable, DataRow[]> func = (table) =>
            {
                return (from x in table.Rows.Cast<DataRow>() select x).ToArray();
            };
            return AddOrReplaceTableRows(func(pDataTable));
        }

        public Int_Exception AddOrReplaceTableRows(DataRow[] pDataTable)
        {
            // make key array
            Func<DataRow, string> fn_pks_string = (data_row) =>
            {
                List<string> pks = new List<string>();
                foreach (int ordinal in (from x in data_row.Table.PrimaryKey.Cast<DataColumn>()
                                         orderby x.Ordinal
                                         select x.Ordinal))
                    pks.Add($"{data_row.Table.Columns[ordinal].ColumnName}='{data_row[ordinal]}'");
                return string.Join(" AND ", pks);
            };
#if false
                Func<DataRow, object[]> fn_pks_objects = (row) =>
                {
                    List<object> pks = new List<object>();
                    foreach (int ordinal in (from x in row.Table.PrimaryKey.Cast<DataColumn>()
                                             orderby x.Ordinal
                                             select x.Ordinal))
                        pks.Add($"{row[ordinal]}");
                    return pks.ToArray();
                };
#endif

            foreach (DataRow row in pDataTable)
            {

                DataRow new_one = this.GetDataTable().NewRow();

                try
                {
                    //new_one.ItemArray = row.ItemArray; // without column check

                    foreach (DataColumn col in new_one.Table.Columns)
                        new_one[col.ColumnName] = row[col.ColumnName]; // with column check

                    //foreach (DataColumn col in new_one.Table.Columns) 
                    //{
                    //    int order_orgin = row.Table.Columns.IndexOf(col.ColumnName);
                    //    int order_new = col.Ordinal;

                    //    Debug.WriteLine($"diff order origin={order_orgin} new={col.Ordinal}");

                    //    bool ok = true;
                    //    // 이름 
                    //    if (0 > order_orgin)
                    //    {
                    //        ok = false;
                    //        Debug.WriteLine($"diff order new={col.Ordinal}={col.ColumnName}={col.DataType.ToString()}");
                    //    }
                    //    else if (row.Table.Columns[order_orgin].DataType.ToString() != col.DataType.ToString())
                    //    {
                    //        ok = false;

                    //        Debug.WriteLine($"diff type origin={row.Table.Columns[order_orgin].ColumnName}={row.Table.Columns[order_orgin].DataType.ToString()} new={col.ColumnName}={col.DataType.ToString()}");
                    //    }

                    //    if (true == ok) new_one[col.ColumnName] = row[col.ColumnName];
                    //}
                }
                catch (Exception ex)
                {
                    // System.ArgumentException
                    // System.InvalidCastException
                    // System.Data.ConstraintException
                    // System.Data.ReadOnlyException
                    // System.Data.NoNullAllowedException
                    // System.Data.DeletedRowInaccessibleException

                    return new Int_Exception(ex);
                }
                try
                {
                    // find use key object[]
                    DataRow[] index = this.GetDataTable().Select(fn_pks_string(new_one));
                    if (0 < index.Length)
                    {
                        index[0].ItemArray = new_one.ItemArray; // replace
                    }
                    else
                    {
                        this.GetDataTable().Rows.Add(new_one); // add
                    }
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }
            }
            // 변경사항을 초기화
            //this.GetDataTable().AcceptChanges();

            return new Int_Exception(this.GetDataTable().Rows.Count);
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

        public int BatchSize { get { return _BatchSize; } set { this._BatchSize = value; } }
        int _BatchSize = int.MaxValue;

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
            this._baseTable = EntityContextRegister.Clone(this.DbConnection, this.DataTableName);
            if (null != this._baseTable)
            {
                return this._baseTable;
            }

            bool isTableExists = false;
            if (this.DbConnection is SqlConnection)
            {
                isTableExists = checkDataTable(this.DataTableName, this.DbConnection, this.DbTransaction);
            }
            if (this.DbConnection is OleDbConnection)
            {
                isTableExists = checkOleDataTable(this.DataTableName, (OleDbConnection)this.DbConnection, (OleDbTransaction)this.DbTransaction);
            }
            if (false == isTableExists)
                return null;

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
            EntityContextRegister.Add(this.DbConnection, this.DataTableName, this._baseTable);

            return this._baseTable;

        }
        #region Insert
        /// <summary>
        /// Insert
        /// </summary>
        /// <returns></returns>
        public Int_Exception Insert()
        {
            string query = $"SELECT * FROM {this.DataTableName}";

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
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }

                try
                {
                    DataRow[] rows;
                    while (0 < (rows = get_data_rows(this.GetDataTable(), DataRowState.Added, this.BatchSize)).Length)
                    {
                        if (this.DbConnection is SqlConnection)
                            ((SqlDataAdapter)adapter).Update(rows);

                        if (this.DbConnection is OleDbConnection)
                            ((OleDbDataAdapter)adapter).Update(rows);
                    };
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
        /// <returns></returns>
        public Int_Exception Update(Hashtable pKeyValue)
        {
            if (null == pKeyValue) pKeyValue = new Hashtable();

            foreach (DataRow row in this.GetDataTable().Rows)
                foreach (string key in pKeyValue.Keys)
                    row[key] = pKeyValue[key];

            string query = $"SELECT * FROM {this.DataTableName}";

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
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }

                try
                {
                    DataRow[] rows;
                    while (0 < (rows = get_data_rows(this.GetDataTable(), DataRowState.Modified, this.BatchSize)).Length)
                    {
                        if (this.DbConnection is SqlConnection)
                            ((SqlDataAdapter)adapter).Update(rows);

                        if (this.DbConnection is OleDbConnection)
                            ((OleDbDataAdapter)adapter).Update(rows);
                    }
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
        /// <returns></returns>
        public Int_Exception Delete()
        {
            string query = $"SELECT * FROM {this.DataTableName}";

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
                }
                catch (Exception ex)
                {
                    return new Int_Exception(ex);
                }

                try
                {
                    DataRow[] rows;
                    while (0 < (rows = get_data_rows(this.GetDataTable(), DataRowState.Deleted, this.BatchSize)).Length)
                    {
                        if (this.DbConnection is SqlConnection)
                            ((SqlDataAdapter)adapter).Update(rows);

                        if (this.DbConnection is OleDbConnection)
                            ((OleDbDataAdapter)adapter).Update(rows);
                    }
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
            if (null == param) param = new Hashtable();

            List<string> columns = new List<string>();
            foreach (DataColumn col in this.Columns)
                columns.Add(string.Format("{0}", col.ColumnName));
            

            List<string> conds = new List<string>();
            foreach (object key in param.Keys)
                conds.Add(string.Format("AND {0}='{1}'", key, param[key]));
            
            string query = $"SELECT {string.Join(", ", columns)} FROM {this.DataTableName}"
                + ((this.DbConnection is OleDbConnection) ? "" : " WITH(NOLOCK)")
                + $" WHERE 1=1 {string.Join(" ", conds)} {string.Join(" ", opt)}";
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
                //SqlBulkCopyOptions opt = SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock;
                SqlBulkCopyOptions opt = SqlBulkCopyOptions.Default | SqlBulkCopyOptions.KeepNulls;

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy((SqlConnection)this.DbConnection, opt, (SqlTransaction)this.DbTransaction))
                {
                    bulkCopy.BulkCopyTimeout = 300;
                    bulkCopy.BatchSize = this.BatchSize;
                    bulkCopy.DestinationTableName = this.DataTableName;

                    foreach (var column in this.Columns)
                        bulkCopy.ColumnMappings.Add(column.ToString(), column.ToString());

                    int nSqlRowsCopied = 0;
                    bulkCopy.SqlRowsCopied += (s, e) => { ++nSqlRowsCopied; };

                    try
                    {
                        DataRow[] rows;
                        while (0 < (rows = get_data_rows(this.GetDataTable(), DataRowState.Added, BatchSize)).Length)
                        {
                            bulkCopy.WriteToServer(rows);

                            // SqlBulkCopy.WriteToServer 함수는 RowState 생태를 변경 시켜주지 않는다 
                            // get_data_rows 함수에서 DataRowState.Added 상태를 중복해서 가져오지 않기위해 강제 변환
                            foreach (DataRow row in rows)
                                row.AcceptChanges();
                        }
                    }
                    //catch (SqlException ex)
                    //{
                    //    // 2601 Violation in unique index
                    //    // 2627 Violation in unique constraint
                    //    if (2601 == ex.Number || 2627 == ex.Number)
                    //    {
                    //    }
                    //    else
                    //    {
                    //        return new Int_Exception(ex);
                    //    }
                    //}
                    catch (Exception ex)
                    {
                        return new Int_Exception(ex);
                    }

                    return new Int_Exception(nSqlRowsCopied);
                }
            }
            else if (this.DbConnection is OleDbConnection)
            {
                string query = $"SELECT * FROM {this.DataTableName} ";

                using (OleDbCommand cmd = (OleDbCommand)this.DbConnection.CreateCommand())
                {
                    cmd.CommandText = query;
                    cmd.CommandType = CommandType.Text;
                    cmd.Transaction = (OleDbTransaction)this.DbTransaction;
                    cmd.CommandTimeout = 300;

                    using (OleDbDataAdapter adapter = new OleDbDataAdapter(cmd))
                    {
                        int nRowUpdated = 0;
                        adapter.RowUpdating += (s, e) => { };
                        adapter.RowUpdated += (s, e) => { ++nRowUpdated; };

                        try
                        {
                            OleDbCommandBuilder oleDbCommandBuilder = new OleDbCommandBuilder(adapter);
                            adapter.InsertCommand = oleDbCommandBuilder.GetInsertCommand();
                            adapter.UpdateCommand = oleDbCommandBuilder.GetUpdateCommand(); // ?
                        }
                        catch (Exception ex)
                        {
                            return new Int_Exception(ex);
                        }

                        try
                        {
                            DataRow[] rows;
                            while (0 < (rows = get_data_rows(this.GetDataTable(), DataRowState.Added, BatchSize)).Length)
                            {
                                adapter.Update(rows);
                            }
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

        Func<DataTable, DataRowState, int, DataRow[]> get_data_rows = (table, state, batch_size) =>
        {
            return (from x in table.Rows.Cast<DataRow>()
                    where x.RowState == (x.RowState & state)
                    select x).Take(batch_size).ToArray();
        };

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
            string q = string.Format($"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_NAME='{pTableName}';");

            using (IDbCommand cmd = pDbConn.CreateCommand())
            {
                cmd.CommandText = q;
                cmd.CommandType = CommandType.Text;
                cmd.Transaction = pDbTran;

                using (DataTable ConstraintColumnUsage = new DataTable())
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    ConstraintColumnUsage.Load(reader);
                    return extractPrimaryKey(pDataTable, ConstraintColumnUsage);
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
            object[] opt = new object[]
            {
                null, // TABLE_CATALOG
                null, // TABLE_SCHEMA
                pTableName, // TABLE_NAME * 
            };
            using (DataTable ConstraintColumnUsage = pDbConn.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, opt))
            {
                return extractPrimaryKey(pDataTable, ConstraintColumnUsage);
            }
        }
        /// <summary>
        /// extractPrimaryKey - 테이블 기본키 가져오기 (공통)
        /// </summary>
        /// <param name="pDataTable"></param>
        /// <param name="pConstraintColumnUsage"></param>
        /// <returns></returns>
        static DataColumn[] extractPrimaryKey(DataTable pDataTable, DataTable pConstraintColumnUsage)
        {
            List<DataColumn> PrimaryKeys = new List<DataColumn>();
            foreach (DataRow row in pConstraintColumnUsage.Rows)
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

        /// <summary>
        /// checkDataTable
        /// </summary>
        /// <param name="pDataTable"></param>
        /// <param name="pTableName"></param>
        /// <param name="pDbConn"></param>
        /// <param name="pDbTran"></param>
        /// <returns></returns>
        static bool checkDataTable(string pTableName, IDbConnection pDbConn, IDbTransaction pDbTran)
        {
            if (0 == pTableName.IndexOf("INFORMATION_SCHEMA."))
                return true; // it is System Information Schema Views

            string q = string.Format($"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{pTableName}';");

            using (IDbCommand cmd = pDbConn.CreateCommand())
            {
                cmd.CommandText = q;
                cmd.CommandType = CommandType.Text;
                cmd.Transaction = pDbTran;

                using (DataTable dt = new DataTable())
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    dt.Load(reader);
                    return 0 < dt.Rows.Count;
                }
            }
        }

        /// <summary>
        /// checkOleDataTable
        /// </summary>
        /// <param name="pDataTable"></param>
        /// <param name="pTableName"></param>
        /// <param name="pDbConn"></param>
        /// <param name="pDbTran"></param>
        /// <returns></returns>
        static bool checkOleDataTable(string pTableName, OleDbConnection pDbConn, OleDbTransaction pDbTran)
        {
            object[] opt = new object[]
            {
                null, // TABLE_CATALOG
                null, // TABLE_SCHEMA
                pTableName, // TABLE_NAME * 
                null, // TABLE_TYPE      
            };

            using (DataTable dt = pDbConn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, opt))
            {
                return 0 < dt.Rows.Count;
            }
        }
        /// <summary>
        /// ExectuteBatch
        /// </summary>
        /// <param name="pQuery"></param>
        /// <param name="pConnection"></param>
        /// <param name="pTransaction"></param>
        /// <returns></returns>
        public static Int_Exception ExectuteBatch(string pQuery, IDbConnection pConnection, IDbTransaction pTransaction = null)
        {
            pQuery = pQuery.Replace("\r\n", "\n");
            pQuery += "\nGO";
            //Debug.WriteLine(this.Query);

            string sqlBatch = string.Empty;
            int nRst = 0;
            foreach (string line in pQuery.Split(new string[2] { "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries))
            {
                string s = line;

                Match match = Regex.Match(s.Trim().ToUpper(), @"^GO$");
                if (true == match.Success)
                {
                    if (false == 0 < sqlBatch.Length)
                        continue;

                    Debug.WriteLine(sqlBatch);
                    using (IDbCommand command = pConnection.CreateCommand())
                    {
                        command.CommandText = sqlBatch;
                        command.CommandType = CommandType.Text;
                        command.Transaction = pTransaction;
                        //command.CommandTimeout

                        try
                        {
                            command.ExecuteNonQuery();
                        }
                        catch (SqlException ex)
                        {
                            return new Int_Exception(ex);
                        }
                        catch (OleDbException ex)
                        {
                            return new Int_Exception(ex);
                        }
                        nRst++;
                        sqlBatch = string.Empty;
                    }
                }
                else
                    sqlBatch += s + "\n";
            }

            return new Int_Exception(nRst);
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
            Exception ex = p.Err;
            if (null != ex)
            {
                BaseFile.WriteLogFile(BaseFile.LOG_FILE_MODE.EXCEP, ex.TargetSite.Module.Assembly.ManifestModule.Name + "." +
                      ex.TargetSite.DeclaringType.Name + "." + ex.TargetSite.Name + "()",
                      "Exception." + ex.Message);
                return true;
            }
            return false;
        }
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

