using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;

namespace W_STD_Formula
{
    public class DBType_MySQL
    {
        //Enum Execute Type
        public enum EXEC
        {
            Reader,
            NonQuery,
            Scalar
        }

        public enum enuCommandType
        {
            StoreProcedure,
            Text
        }

        public enum eData
        {
            DataReader,
            DataAdapter
        }

        public enum eCommandType
        {
            Text,
            StoredProcedure
        }

        //Variable
        private DataTable fldDataTable = new DataTable();
           private int fldConnectionTimeout = 0;

        //DB Operation Variable

        public MySqlConnection Connection = new MySqlConnection(W_Module.ConnectionString);
        public MySqlDataAdapter DataAdapter = new MySqlDataAdapter();

        public MySqlCommand MySQLCommand = new MySqlCommand();
        public DataSet DS = new DataSet();


        private MySqlDataReader dmssql_DataReader;
        private MySqlDataAdapter dmssql_DataAdapter = new MySqlDataAdapter();
        private string dmssql_ScalarResult = "";
        public bool connectDB() {
            try
            {
                if (Connection.State == ConnectionState.Closed) {
                    Connection.Open();
                    return true;
                }

                return false;
            }
            catch (MySqlException ex) {
                throw new Exception("Database Exception \n" + ex.Message);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void disconnectDB() {
            try
            {
                if (Connection.State == ConnectionState.Open)
                    Connection.Close();
            }
            catch (MySqlException ex)
            {
                throw new Exception("Database Exception \n" + ex.Message);
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public void DBconnect() {
            if (Connection.State == ConnectionState.Open)
                Connection.Close();

            Connection.Open();
        }

        public void DBdisconnect() {
            Connection.Close();
        }

        public DataTable DBExeQuery(string StrSQL, eCommandType eCommandType = eCommandType.Text, eData edata = eData.DataReader, int eCommandTimeout = 0) {
            DataTable dt = new DataTable();
            try
            {
                MySQLCommand.CommandText = StrSQL;
                MySQLCommand.Connection = Connection;

                if (eCommandTimeout == 0)
                {
                    MySQLCommand.CommandTimeout = fldConnectionTimeout;
                }
                else {
                    MySQLCommand.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure) {
                    MySQLCommand.CommandType = CommandType.StoredProcedure;
                }
                else {
                    MySQLCommand.CommandType = CommandType.Text;
                }

                switch (edata)
                {
                    case eData.DataReader:
                        DBconnect();
                        dmssql_DataReader = MySQLCommand.ExecuteReader();
                        dt.Load(dmssql_DataReader);
                        dmssql_DataReader.Close();
                        DBdisconnect();
                        break;
                    case eData.DataAdapter:
                        dmssql_DataAdapter.SelectCommand = MySQLCommand;
                        dmssql_DataAdapter.Fill(dt);
                        break;
                    default:
                        break;
                }

                return dt;
            }
            catch (Exception ex)
            {

                throw ex;
            }

        }

        public DataTable DBExeQuery(string StrSQL, MySqlConnection Connection, MySqlTransaction Transaction, eCommandType eCommandType = eCommandType.Text, eData edata = eData.DataReader, int eCommandTimeout = 0) {
            DataTable dt = new DataTable();

            try
            {
                MySQLCommand.CommandText = StrSQL;
                MySQLCommand.Connection = Connection;
                MySQLCommand.Transaction = Transaction;

                if (eCommandTimeout == 0) {
                    MySQLCommand.CommandTimeout = fldConnectionTimeout;
                }else {
                    MySQLCommand.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    MySQLCommand.CommandType = CommandType.StoredProcedure;
                }else {
                    MySQLCommand.CommandType = CommandType.Text;
                }

                switch (edata)
                {
                    case eData.DataReader:
                        dmssql_DataReader = MySQLCommand.ExecuteReader();
                        dt = GetDrToDTManuel(dmssql_DataReader);
                        break;
                    case eData.DataAdapter:
                        dmssql_DataAdapter.SelectCommand = MySQLCommand;
                        dmssql_DataAdapter.Fill(dt);
                        break;
                    default:
                        break;
                }

                return dt;
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public DataTable DBExeQuery(string StrSQL, MySqlConnection Connection, MySqlTransaction Transaction, MySqlCommand pMySqlCommand, eCommandType eCommandType = eCommandType.Text, eData edata = eData.DataReader, int eCommandTimeout = 0) {
            DataTable dt = new DataTable();
            try
            {
                pMySqlCommand.CommandText = StrSQL;
                pMySqlCommand.Connection = Connection;
                pMySqlCommand.Transaction = Transaction;

                if (eCommandTimeout == 0)
                {
                    pMySqlCommand.CommandTimeout = fldConnectionTimeout;
                }else {
                    pMySqlCommand.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    pMySqlCommand.CommandType = CommandType.StoredProcedure;
                }else {
                    pMySqlCommand.CommandType = CommandType.Text;
                }

                switch (edata)
                {
                    case eData.DataAdapter:
                        DataAdapter.SelectCommand = pMySqlCommand;
                        DataAdapter.Fill(dt);
                        break;
                    default:
                        dmssql_DataReader = pMySqlCommand.ExecuteReader();
                        dt = GetDrToDTManuel(dmssql_DataReader);
                        dmssql_DataReader.Close();
                        break;
                }

                return dt;
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
        public DataTable GetDrToDTManuel(MySqlDataReader dr) {
            DataTable dt = new DataTable();
            GC.Collect();
            DataTable dtSchemar = dr.GetSchemaTable();
            List<DataColumn> listCols = new List<DataColumn>();

            if (dtSchemar != null) {
                foreach (DataRow item in dtSchemar.Rows)
                {
                    string columnName = System.Convert.ToString(item["ColumnName"]);
                    DataColumn column = new DataColumn(columnName, (Type)item["DataType"]);
                    column.ReadOnly = false;
                    column.Unique = (bool)item["IsUnique"];
                    column.AllowDBNull = (bool)item["AllowDBNull"];
                    column.AutoIncrement = (bool)item["IsAutoIncrement"];

                    listCols.Add(column);
                    dt.Columns.Add(column);
                }

                do
                {
                    DataRow dataRow = dt.NewRow();
                    for (int i = 0; i < listCols.Count - 1; i++)
                    {
                        dataRow[(DataColumn)listCols[i]] = dr[i];
                    }

                    dt.Rows.Add(dataRow);
                } while (dr.Read());
            }

            return dt;
        }

        public string DBExeQuery_Scalar(string StrSQL, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0) {
            try
            {
                DBconnect();
                MySQLCommand.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    MySQLCommand.CommandTimeout = fldConnectionTimeout;
                }else {
                    MySQLCommand.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    MySQLCommand.CommandType = CommandType.StoredProcedure;
                }else {
                    MySQLCommand.CommandType = CommandType.Text;
                }

                MySQLCommand.Connection = Connection;

                dmssql_ScalarResult = (string)MySQLCommand.ExecuteScalar();

                DBdisconnect();

                return dmssql_ScalarResult;
            }
            catch (Exception ex)
            {

                throw ex;
            }

        }

        public string DBExeQuery_Scalar(string StrSQL, MySqlConnection Connection, MySqlTransaction Transaction, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0) {
            try
            {
                MySQLCommand.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    MySQLCommand.CommandTimeout = fldConnectionTimeout;
                }
                else {
                    MySQLCommand.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    MySQLCommand.CommandType = CommandType.StoredProcedure;
                }
                else {
                    MySQLCommand.CommandType = CommandType.Text;
                }

                MySQLCommand.Connection = Connection;
                MySQLCommand.Transaction = Transaction;

                dmssql_ScalarResult = (string)MySQLCommand.ExecuteScalar();

                return dmssql_ScalarResult;
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public string DBExeQuery_Scalar(string StrSQL, MySqlConnection Connection, MySqlTransaction Transaction, MySqlCommand pMySQLCommand, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0) {
            try
            {
                pMySQLCommand.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    pMySQLCommand.CommandTimeout = fldConnectionTimeout;
                }
                else {
                    pMySQLCommand.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    pMySQLCommand.CommandType = CommandType.StoredProcedure;
                }
                else {
                    pMySQLCommand.CommandType = CommandType.Text;
                }

                pMySQLCommand.Connection = Connection;
                pMySQLCommand.Transaction = Transaction;

                dmssql_ScalarResult = (string)pMySQLCommand.ExecuteScalar();

                return dmssql_ScalarResult;
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public int DBExeNonQuery(string StrSQL, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0) {
            int dmssql_NonQueryResult;
            try
            {
                DBconnect();
                MySQLCommand.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    MySQLCommand.CommandTimeout = fldConnectionTimeout;
                }
                else {
                    MySQLCommand.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    MySQLCommand.CommandType = CommandType.StoredProcedure;
                }
                else {
                    MySQLCommand.CommandType = CommandType.Text;
                }

                MySQLCommand.Connection = Connection;
                dmssql_NonQueryResult = MySQLCommand.ExecuteNonQuery();

                DBdisconnect();

                return dmssql_NonQueryResult;
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public int DBExeNonQuery(string StrSQL, MySqlConnection Connection, MySqlTransaction Transaction, eCommandType eCommandType = eCommandType.Text,int eCommandTimeout = 0) {
        
            try
            {
                MySQLCommand.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    MySQLCommand.CommandTimeout = fldConnectionTimeout;
                }
                else {
                    MySQLCommand.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    MySQLCommand.CommandType = CommandType.StoredProcedure;
                }
                else {
                    MySQLCommand.CommandType = CommandType.Text;
                }

                MySQLCommand.Connection = Connection;
                MySQLCommand.Transaction = Transaction;

                return MySQLCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public int DBExeNonQuery(string StrSQL, MySqlConnection Connection, MySqlTransaction Transaction, MySqlCommand pMySQLCommand, eCommandType eCommandType = eCommandType.Text, int eCommandTimeout = 0) {
            try
            {
                pMySQLCommand.CommandText = StrSQL;

                if (eCommandTimeout == 0)
                {
                    pMySQLCommand.CommandTimeout = fldConnectionTimeout;
                }
                else {
                    pMySQLCommand.CommandTimeout = eCommandTimeout;
                }

                if (eCommandType == eCommandType.StoredProcedure)
                {
                    pMySQLCommand.CommandType = CommandType.StoredProcedure;
                }
                else {
                    pMySQLCommand.CommandType = CommandType.Text;
                }

                pMySQLCommand.Connection = Connection;
                pMySQLCommand.Transaction = Transaction;

                return pMySQLCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
    }
}
