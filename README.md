# EntityModel

## c# DataBase Connection With SqlConnection and OleDbConnection 

### SqlConnection
<pre>
<code>
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;

public int Insert(DataTable data)
{
    using (SqlCommand command = connection.CreateCommand())
    {
        command.CommandText = "SELECT * FROM +" sTableName;
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
            adapter.Update(data)
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return nRowUpdated;
        }
    }
}
</code>
</pre>
### OleDbConnection
<pre>
<code>

</code>
</pre>
## FUNCTIONS
