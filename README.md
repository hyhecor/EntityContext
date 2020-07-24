# EntityContext

## c# DataBase Connection With SqlConnection and OleDbConnection 

### SqlConnection
<pre>
<code>
using System;
using System.Data;
using System.Data.SqlClient;

class Context
{
    public int Insert(DataTable table, int batchSize, SqlConnection connection, SqlTransaction trasection = null)
    {
        using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = $"SELECT * FROM {table.TableName}";
            command.CommandType = CommandType.Text;
            command.Transaction = trasection;

            int nRowUpdated = 0;
            using (SqlDataAdapter adapter = new SqlDataAdapter(command))
            using (SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter))
            {
                adapter.RowUpdated += (s, e) => { ++nRowUpdated; };
                try
                {
                    adapter.InsertCommand = commandBuilder.GetInsertCommand();
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                try
                {
                    DataRow[] rows;
                    while (0 < (rows = get_data_rows(table, DataRowState.Added, batchSize)).Length)
                    {
                        adapter.Update(rows);
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                return nRowUpdated;
            }
        }
    }
    
    Func<DataTable, DataRowState, int, DataRow[]> get_data_rows = (table, state, batchSize) =>
    {
        return (from x in table.Rows.Cast<DataRow>()
                where x.RowState == (x.RowState & state)
                select x).Take(batchSize).ToArray();
    };
}

</code>
</pre>
### OleDbConnection
<pre>
<code>

</code>
</pre>
## FUNCTIONS
