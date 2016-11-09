#region Help:  Introduction to the script task
/* The Script Task allows you to perform virtually any operation that can be accomplished in
 * a .Net application within the context of an Integration Services control flow. 
 * 
 * Expand the other regions which have "Help" prefixes for examples of specific ways to use
 * Integration Services features within this script task. */
#endregion


#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.IO;
using System.Data.SqlClient;
using System.Text;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
#endregion

namespace ST_9fe3f0c506744f589075f4e22c70b50b
{
    /// <summary>
    /// ScriptMain is the entry point class of the script.  Do not change the name, attributes,
    /// or parent of this class.
    /// </summary>
	[Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
	public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
	{
        #region Help:  Using Integration Services variables and parameters in a script
        /* To use a variable in this script, first ensure that the variable has been added to 
         * either the list contained in the ReadOnlyVariables property or the list contained in 
         * the ReadWriteVariables property of this script task, according to whether or not your
         * code needs to write to the variable.  To add the variable, save this script, close this instance of
         * Visual Studio, and update the ReadOnlyVariables and 
         * ReadWriteVariables properties in the Script Transformation Editor window.
         * To use a parameter in this script, follow the same steps. Parameters are always read-only.
         * 
         * Example of reading from a variable:
         *  DateTime startTime = (DateTime) Dts.Variables["System::StartTime"].Value;
         * 
         * Example of writing to a variable:
         *  Dts.Variables["User::myStringVariable"].Value = "new value";
         * 
         * Example of reading from a package parameter:
         *  int batchId = (int) Dts.Variables["$Package::batchId"].Value;
         *  
         * Example of reading from a project parameter:
         *  int batchId = (int) Dts.Variables["$Project::batchId"].Value;
         * 
         * Example of reading from a sensitive project parameter:
         *  int batchId = (int) Dts.Variables["$Project::batchId"].GetSensitiveValue();
         * */

        #endregion

        #region Help:  Firing Integration Services events from a script
        /* This script task can fire events for logging purposes.
         * 
         * Example of firing an error event:
         *  Dts.Events.FireError(18, "Process Values", "Bad value", "", 0);
         * 
         * Example of firing an information event:
         *  Dts.Events.FireInformation(3, "Process Values", "Processing has started", "", 0, ref fireAgain)
         * 
         * Example of firing a warning event:
         *  Dts.Events.FireWarning(14, "Process Values", "No values received for input", "", 0);
         * */
        #endregion

        #region Help:  Using Integration Services connection managers in a script
        /* Some types of connection managers can be used in this script task.  See the topic 
         * "Working with Connection Managers Programatically" for details.
         * 
         * Example of using an ADO.Net connection manager:
         *  object rawConnection = Dts.Connections["Sales DB"].AcquireConnection(Dts.Transaction);
         *  SqlConnection myADONETConnection = (SqlConnection)rawConnection;
         *  //Use the connection in some code here, then release the connection
         *  Dts.Connections["Sales DB"].ReleaseConnection(rawConnection);
         *
         * Example of using a File connection manager
         *  object rawConnection = Dts.Connections["Prices.zip"].AcquireConnection(Dts.Transaction);
         *  string filePath = (string)rawConnection;
         *  //Use the connection in some code here, then release the connection
         *  Dts.Connections["Prices.zip"].ReleaseConnection(rawConnection);
         * */
        #endregion


        public static List<string> splitCSV(string line)
        {
            List<string> buff = new List<string>();
            string accum = "";
            bool escape = false;
            for (int i = 0; i < line.Length; i++)
            {
                char at = line[i];
                if (! escape)
                {
                    if (at == '"') //Start of an escape sequence
                    {
                        escape = true;
                    }
                    else if (at == ',') //Column has ended, add it to buffer
                    {
                        buff.Add(accum);
                        accum = "";
                    }
                    else //Normal character, add it to the accumulator
                    {
                        accum += at;
                    }
                }
                else //We are in an escape sequence
                {
                    bool end = (i + 1 == line.Length);
                    if (at == '"' && (end || line[i + 1] == ',')) //Encountered end of escape sequence, fall back to standard parsing
                    {
                        escape = false; 
                    }
                    else if (at == '"' && line[i + 1] == '"') //Encountered double ", skip the current quote 
                    {
                        accum += '"';
                        i += 1;
                    }
                    else //Just another character, add it to the accumulator
                    {
                        accum += at;
                    }
                }
            }

            buff.Add(accum); //Make sure we add the last column into the buffer (CSV dont end with a ,)
            return buff;
        }

        public static List<string> splitDefault(string line, char delimiter)
        {
            List<string> buff = new List<string>();
            foreach (string col in line.Split(delimiter))
            {
                buff.Add(col);
            }
            return buff;
        }

        public static string normalize(string line, char delimiter)
        {
            List<string> columns = (delimiter == ',' ? splitCSV(line) : splitDefault(line, delimiter));
            List<string> buff = new List<string>();
            foreach (string col in columns)
            {
                buff.Add("'" + col.Replace("'", "''") + "'");
            }

            return string.Join(",", buff);
        }

        private static string logFolder = null;

        /// <summary>
        /// This method is called when this script task executes in the control flow.
        /// Before returning from this method, set the value of Dts.TaskResult to indicate success or failure.
        /// To open Help, press F1.
        /// </summary>
        public void Main()
        {

            string datetime = DateTime.Now.ToString("yyyyMMddHHmmss");

            //Declare Variables
            string SourceFolderPath = Dts.Variables["User::SourceFolder"].Value.ToString();
            logFolder = Dts.Variables["User::LogFolder"].Value.ToString();
            string FileExtension = Dts.Variables["User::FileExtension"].Value.ToString();
            string FileDelimiter = Dts.Variables["User::FileDelimiter"].Value.ToString();
            string TableName = Dts.Variables["User::DestinationTable"].Value.ToString();
            string ArchiveFolder = Dts.Variables["User::ArchiveFolder"].Value.ToString();
            //string ColumnList = "";

            bool success = true;

            //Reading file names one by one
            string SourceDirectory = SourceFolderPath;
            string[] fileEntries = Directory.GetFiles(SourceDirectory, "*" + FileExtension);

            using (StreamWriter OUT = new StreamWriter(Dts.Variables["User::LogFolder"].Value.ToString() + "\\" + "OutputLog_" + datetime + ".log", true))
            {
                DateTime startAll = DateTime.Now;
                Parallel.ForEach(fileEntries, (fileName, loopState) =>
                {
                    SqlConnection myADONETConnection = new SqlConnection();
                    using (myADONETConnection = (SqlConnection)(Dts.Connections["DB_Conn_StagingArea"].AcquireConnection(Dts.Transaction) as SqlConnection))
                    {
                        try
                        {
                            //Writing Data of File Into Table
                            int counter = 0;
                            string line;
                            //MessageBox.Show(fileName);

                            DateTime start = DateTime.Now;
                            using (System.IO.StreamReader SourceFile = new System.IO.StreamReader(fileName))
                            {
                                while ((line = SourceFile.ReadLine()) != null)
                                {
                                    //TODO: HACK TO CLEAN OFF NORMALIZATION IN EXCEL REMOVE ME!!
                                    line = line.Replace(":apos", "'").Replace(":comma", ",");

                                    //MessageBox.Show(line.ToString());
                                    if (counter > 0)
                                    {
                                        string query = "Insert into " + TableName + " Values (" +
                                                        normalize(line, FileDelimiter.ToCharArray()[0]) +
                                                        ",'" + fileName + "')";

                                        //MessageBox.Show(query.ToString());
                                        SqlCommand myCommand1 = new SqlCommand(query, myADONETConnection);
                                        myCommand1.ExecuteNonQuery();
                                    }

                                    counter++;
                                }
                            }
                            OUT.WriteLine("Took " + (DateTime.Now - start).TotalSeconds + " seconds to execute " + counter + " import statements for " + fileName + ".");

                            //move the file to archive folder after adding datetime to it
                            File.Move(fileName, ArchiveFolder + "\\" + (fileName.Replace(SourceFolderPath, "")).Replace(FileExtension, "") + "_" + datetime + FileExtension);

                            OUT.WriteLine("Took " + (DateTime.Now - start).TotalSeconds + " seconds to import and archive " + fileName + ".");
                        }
                        catch (Exception exception)
                        {
                            loopState.Break();

                            // Create Log File for Errors
                            using (StreamWriter sw = new StreamWriter(Dts.Variables["User::LogFolder"].Value.ToString()
                                + "\\" + "ErrorLog_" + datetime + ".log", true))
                            {
                                sw.WriteLine(exception.ToString());
                                success = false;
                            }
                        }
                    }
                });//END Paralell add

                OUT.WriteLine("Took " + (DateTime.Now - startAll).TotalSeconds + " seconds to import " + fileEntries.Length + " raw files.");
                OUT.WriteLine(success ? "Success!!" : "Failure :C");
            }

            if (success)
            {
                Dts.TaskResult = (int)ScriptResults.Success;
            }
            else
            {
                Dts.TaskResult = (int)ScriptResults.Failure;
            }
        }

        #region ScriptResults declaration
        /// <summary>
        /// This enum provides a convenient shorthand within the scope of this class for setting the
        /// result of the script.
        /// 
        /// This code was generated automatically.
        /// </summary>
        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        };
        #endregion

	}
}
