using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace MirrorMapper.Procedures
{
    public static class MirrorProcedure
    {
        public static List<T> Execute<T>(DbConnection connection, object args,
            [System.Runtime.CompilerServices.CallerMemberName] string procName = null) where T : new()
        {
            // ReSharper disable once ExplicitCallerInfoArgument
            return Mirror.Map<T>(Execute(connection, args, procName));
        }

        public static DataTable Execute(DbConnection connection, IDictionary<string, object> args, [System.Runtime.CompilerServices.CallerMemberName] string procName = null)
        {
            if (procName != null)
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = procName;
                cmd.CommandType = CommandType.StoredProcedure;
                foreach (var kvp in args) {
                    var param = cmd.CreateParameter();
                    param.ParameterName = $"@{kvp.Key}";
                    param.Value = kvp.Value ?? DBNull.Value;
                    cmd.Parameters.Add(param);
                }

                var dataTable = Exec(cmd);
                return dataTable;
            }
            return null;
        }

        public static DataTable Execute(DbConnection connection, object args, [System.Runtime.CompilerServices.CallerMemberName] string procName = null)
        {
            if (procName != null)
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = procName;
                cmd.CommandType = CommandType.StoredProcedure;
                foreach (var prop in args.GetType().GetProperties())
                {
                    var param = cmd.CreateParameter();
                    param.ParameterName = $"@{prop.Name}";
                    param.Value = prop.GetValue(args) ?? DBNull.Value;
                    cmd.Parameters.Add(param);
                }

                var dataTable = Exec(cmd);
                return dataTable;
            }
            return null;
        }

        private static DataTable Exec(DbCommand cmd)
        {
            DataTable result = new DataTable();
            DbConnection conn = cmd.Connection;
            try {
                conn.Open();
                DbDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                DataTable dtSchema = reader.GetSchemaTable();
                List<DataColumn> listCols = new List<DataColumn>();
                Dictionary<string, int> dupColNameIndex = new Dictionary<string, int>();

                // Add columns
                if (dtSchema != null) {
                    foreach (DataRow schemaRow in dtSchema.Rows) {
                        string columnName = Convert.ToString(schemaRow["ColumnName"]);

                        // Handle blank and duplicate column names
                        bool emptyColName = string.IsNullOrEmpty(columnName);
                        if (emptyColName) columnName = "Column";
                        if (dupColNameIndex.TryGetValue(columnName, out int dupIndex)) {
                            dupColNameIndex[columnName] = ++dupIndex;
                            columnName += dupIndex;
                        }
                        else {
                            int index = 0;
                            if (emptyColName) columnName += ++index;
                            dupColNameIndex[columnName] = index;
                        }

                        // Create and add the column
                        DataColumn column = new DataColumn(columnName, (Type) (schemaRow["DataType"]));
                        column.Unique = (bool) schemaRow["IsUnique"];
                        column.AllowDBNull = (bool) schemaRow["AllowDBNull"];
                        column.AutoIncrement = (bool) schemaRow["IsAutoIncrement"];
                        listCols.Add(column);
                        result.Columns.Add(column);
                    }
                }

                // Add rows
                while (reader.Read()) {
                    DataRow dataRow = result.NewRow();
                    for (int i = 0; i < listCols.Count; i++) {
                        dataRow[listCols[i]] = reader[i];
                    }

                    result.Rows.Add(dataRow);
                }

                return result;
            }
            catch (Exception) {
                throw;
            }
            finally
            {
                conn.Close();
            }
        }
    }
}
