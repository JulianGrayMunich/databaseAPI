using System.IO;
using System.Data.SqlClient;
using GNAspreadsheettools;
using System;



#pragma warning disable

namespace databaseAPI
{
    public class dbAPI
    {


        public void testDBconnection(string strDBconnection)
        {
            // 
            // Purpose
            //  To test a DB Connection
            // Useage
            //  dbAPI gnaDBAPI = new dbAPI();
            //  gnaDBAPI.testDBconnection(strDBconnection);
            // Output
            //  Successful or Failed with error message
            //
            // If you suddenly start having failed connections, check that the versions of the SQL packages match
            // between dbAPI and the calling software
            // If they dont, then update so that they both match
            //
            //instantiate and open connection

            //======================================

            try
            {
                //sql connection object
                using SqlConnection conn = new(strDBconnection);
                conn.Open();
                Console.WriteLine("     DB Connection Successful");
                conn.Dispose();
                conn.Close();
            }
            catch (SqlException ex)
            {
                Console.WriteLine("        DB connection failed: \n        " + ex.Message + "\n");
            }
        }


        //====[Maintenance methods]===


        public void clearTable(string strDBconnection, string strTableName)
        {

            // Purpose:
            //      To clear all data in a table in the DB
            // Input:
            //      DB connection string, table name
            // Output:
            //      None
            // Useage:
            //     gna.clearTable(strDBconnection, strTableName);
            //

            // Connection and Reader declared outside the try block
            using SqlConnection conn = new(strDBconnection);

            //instantiate and open connection
            conn.Open();
            try
            {
                // define the SQL action
                string SQLaction = "TRUNCATE TABLE " + strTableName;
                SqlCommand cmd = new(SQLaction, conn);
                // define the parameter used in the command object and add to the command
                cmd.ExecuteNonQuery();
            }

            catch (System.Data.SqlClient.SqlException ex)
            {
                Console.WriteLine("clearTable: DB connection failed : ");
                Console.WriteLine(ex);
                Console.ReadKey();
            }

            finally
            {
                conn.Dispose();
                conn.Close();
            }
        }

        //====[Get methods]============


        public string getLongNameID(string strDBconnection, string strLongName)
        {

            // Purpose:
            //      To retrieve a single result from the database table
            // Input:
            //      as above
            // Output:
            //      Returns the result as a string
            // Useage:
            //      string strResult = gna.getLongNameID(strDBconnection, strLongName);
            //

            string strResult = "empty";



            // Connection and Reader declared outside the try block
            using SqlConnection conn = new(strDBconnection);
            conn.Open();
            try
            {

                string SQLaction = @"
                SELECT ID 
                FROM fixedData 
                WHERE fixedData.longName = @ELEMENT
                ";

                SqlCommand cmd = new(SQLaction, conn);

                cmd.Parameters.Add(new SqlParameter("@ELEMENT", strLongName));

                // get the value
                strResult = Convert.ToString(cmd.ExecuteScalar());

            }

            catch (System.Data.SqlClient.SqlException ex)
            {
                Console.WriteLine("getLongNameID failed: ");
                Console.WriteLine(ex);
                Console.ReadKey();
            }

            finally
            {
                conn.Dispose();
                conn.Close();
            }

            strResult = strResult.Trim();
            if (strResult.Length < 1) strResult = "Missing";

            return strResult;
        }


        public List<string> getFaultIndicators(string strDBconnectionT4DDB, string strTimeBlockStart, string strTimeBlockEnd)
        {
            // Purpose
            //  to locate any fault indicators in time block
            // Output
            //  

            // select all SourceName where ResourceName=NotifyClient_CallbackDeadlock between TimeStart and TimeEnd

            string strResourceName = "NotifyClient_CallbackDeadlock";
            string strString;
            List<string> strSourceName = new();



            using (SqlConnection conn = new(strDBconnectionT4DDB))
            {
                //open connection
                conn.Open();
                try
                {
                    // define the SQL query,
                    string SQLaction = @"
                SELECT SourceName FROM dbo.Logging
                WHERE ResourceName = @ResourceName 
                AND LogTime BETWEEN " + strTimeBlockStart +
                " AND " + strTimeBlockEnd;


                    SqlCommand cmd = new(SQLaction, conn);

                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@ResourceName", strResourceName));

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    while (dataReader.Read())
                    {
                        strString = (string)dataReader["SourceName"];
                        strString = strString.Replace("Terrestrial Engine RT", "").Replace("[", "").Replace("]", "").Trim();
                        strSourceName.Add(strString);
                    }
                    strSourceName.Add("TheEnd");
                    strSourceName = strSourceName.Distinct().ToList();

                    // Close the dataReader
                    dataReader?.Close();

                }
                catch (SqlException ex)
                {
                    Console.WriteLine("getFaultIndicators: SQL selection failed: ");
                    Console.WriteLine(ex);
                    Console.WriteLine("\nPress key...");
                    Console.ReadKey();
                }
                finally
                {
                    conn.Dispose();
                    conn.Close();
                }
            }

            return strSourceName;


        }

        public Tuple<double, double> extractAverageDistance(string strDBconnection, string strTimeBlockStart, string strTimeBlockEnd, string strSensorName, string strProjectTitle)
        {
            //
            // Purpose:
            //      To extract the average raw distance from dbo.TMTDistance table for the time block strTimeBlockStart to strTimeBlockEnd
            // 
            // Input:
            //      Receives 
            //          the target name
            //          the start and end time blocks
            // Output:
            //      Returns tuple<double,double> <average distance, no of distances used> 
            // Useage:
            //              var answer = gna.extractAverageDistance(strDBconnection, strTimeBlockStart, strTimeBlockEnd, strSensorName);
            //              double dblAverageDistance = answer.Item1;
            //              double dblNoOfElements = answer.Item2;  // this is actually an integer
            // Comment:
            //      If missing then 
            //          average distance = 0.0
            //          no of elements = -99
            //          
            //  20221015: the SQL expression completely re-written to include inner joins
            //

            double dblDistanceCounter = 0.0;
            double dblAverageDistance = 0.0;
            double dblRawDistance;


            //Console.WriteLine(strSensorName);


            // get the projectID
            string strProjectID = getProjectID(strDBconnection, strProjectTitle);

            //instantiate and open connection
            SqlConnection conn = new(strDBconnection);
            conn.Open();

            try
            {

                string SQLaction = @"
                SELECT
                    RawDistance,
                    EndTimeUTC
                FROM ((TMTDistance
                INNER JOIN TMCSensor 
                    ON TMTDistance.SensorID = TMCSensor.ID 
                    AND TMCSensor.Name = @sensorName
                    )
                INNER JOIN TMCLocation 
                    ON TMCSensor.LocationID = TMCLocation.ID 
                    AND TMCLocation.ProjectID = @ProjectID
                    )
                WHERE [TMTDistance].[IsOutlier] = 0
                AND TMTDistance.EndTimeUTC BETWEEN " + strTimeBlockStart + " AND " + strTimeBlockEnd;


                SqlCommand cmd = new(SQLaction, conn);

                // define the parameter used in the command object and add to the command
                // I use this form as I am including a Unicode character in the Select statement

                var par = new SqlParameter("@sensorName", System.Data.SqlDbType.NVarChar)
                {
                    Value = strSensorName
                };

                cmd.Parameters.Add(new SqlParameter("@ProjectID", strProjectID));

                // Define the data reader
                SqlDataReader dataReader = cmd.ExecuteReader();

                // get the values
                while (dataReader.Read())
                {
                    dblRawDistance = (double)dataReader["RawDistance"];
                    dblAverageDistance += dblRawDistance;
                    dblDistanceCounter++;
                }

                // Close the dataReader
                dataReader?.Close();
            }
            catch (System.Data.SqlClient.SqlException ex)
            {
                Console.WriteLine("extractAverageDistance: DB Connection Failed: ");
                Console.WriteLine(ex);
                Console.ReadKey();
            }

            finally
            {
                conn.Dispose();
                conn.Close();
            }

            if (dblDistanceCounter > 0)
            {
                dblAverageDistance = Math.Round(dblAverageDistance / dblDistanceCounter, 4);
            }
            else
            {
                dblAverageDistance = 0.0;
                dblDistanceCounter = -99.0;
            }

            // Console.WriteLine("extractAverageDistance: "+strSensorName+" - "+ Convert.ToString(dblAverageDistance));

            return new Tuple<double, double>(dblAverageDistance, dblDistanceCounter);

        }

        public string getProjectID(string strDBconnection, string strProjectTitle)
        {
            // Purpose:
            //      To determine the project ID from TMCMonitoringProjects
            // Input:
            //      Receives Project Title
            // Output:
            //      Returns Project ID
            // Useage:
            //      string strProjectID = gna.getProjectID(strDBconnection, strProjectTitle);
            //

            string strProjectID = "";
            Int16 iCounter = 0;

            // Connection and Reader declared outside the try block
            using (SqlConnection conn = new(strDBconnection))
            {

                //instantiate and open connection
                conn.Open();

                try
                {
                    // define the SQL query
                    string SQLaction = @"
                    SELECT ID, ProjectTitle  
                    FROM dbo.TMCMonitoringProjects 
                    WHERE TMCMonitoringProjects.ProjectTitle = @ProjectName
                    AND TMCMonitoringProjects.IsDeleted = 0
                    ";
                    SqlCommand cmd = new(SQLaction, conn);

                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@ProjectName", strProjectTitle));

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    while (dataReader.Read())
                    {
                        int iProjectID = (Int32)dataReader["ID"];
                        strProjectID = Convert.ToString(iProjectID);
                        iCounter++;
                    }


                    // Close the dataReader
                    dataReader?.Close();

                }

                catch (System.Data.SqlClient.SqlException ex)
                {
                    Console.WriteLine("getPointCoordinates: DB Connection Failed when retrieving Project ID => Project name not correct : ");
                    Console.WriteLine(ex);
                    Console.ReadKey();
                }

                finally
                {
                    conn.Dispose();
                    conn.Close();
                }
            }

            if (iCounter == 0) { strProjectID = "Missing"; }

            return strProjectID;
        }



        public int getNoOfObservations(string strDBconnection, string strSensorID, string strTimeBlockStart, string strTimeBlockEnd)
        {
            // Purpose
            //  To count the number of observations on a prism in the timeblock
            // Output
            //  No of observations

            int iCount = -99;

            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                try
                {
                    // define the SQL query . Do not try MAX()

                    string SQLaction = @"
                SELECT COUNT(*) FROM dbo.TMTPosition_Terrestrial  
                WHERE SensorID = @SensorID 
                AND IsOutlier = 0 
                AND EndTimeUTC BETWEEN " + strTimeBlockStart +
                " AND " + strTimeBlockEnd;


                    SqlCommand cmd = new(SQLaction, conn);

                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@SensorID", strSensorID));

                    // Define the data reader

                    iCount = Convert.ToInt16(cmd.ExecuteScalar());

                    if (iCount < 0)
                    {
                        iCount = 0;
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("getNoOfObservations: SQL selection failed: ");
                    Console.WriteLine(ex);
                    Console.WriteLine("\nPress key...");
                    Console.ReadKey();
                }
                finally
                {
                    conn.Dispose();
                    conn.Close();
                }
            }

            return iCount;
        }


        public string[,] getLocationID_DO_NOT_USE(string strDBconnection, string strProjectID, string[] strPointNames)
        {
            //
            // Purpose:
            //      To extract the location ID from TMCLocation table: FAULT - must rewrite to exclude matching location name to sensor name
            // Input:
            //      Receives array of point names extracted from the workbook
            // Output:
            //      Returns array [PointName,LocationID]
            // Useage:
            //      string[,] strNamesID = gna.getLocationID(strDBconnection, strProjectTitle, strPointNames);
            // Comment:
            //      If missing then ID="Missing"
            //      last point in list = "NoMore"
            //

            string[,] strPointID = new string[5000, 2];
            string strPointName;
            int iCounter = 0;

            strPointName = strPointNames[iCounter];

            using (SqlConnection conn = new(strDBconnection))
            {
                //instantiate and open connection
                conn.Open();
                do
                {
                    // define the SQL query
                    string SQLaction = @"
                    SELECT TMCLocation.ID  
                    FROM dbo.TMCLocation 
                    WHERE TMCLocation.Name = @Name
                    AND TMCLocation.ProjectID = @ProjectID";

                    SqlCommand cmd = new(SQLaction, conn);

                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@Name", strPointName));
                    cmd.Parameters.Add(new SqlParameter("@ProjectID", strProjectID));

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    // If the point exists, there will be a result. If not, then the point get marked missing

                    if (dataReader.HasRows)
                    {
                        while (dataReader.Read())
                        {
                            int iLocationID = (Int32)dataReader["ID"];
                            strPointID[iCounter, 0] = strPointName;
                            strPointID[iCounter, 1] = Convert.ToString(iLocationID);
                        }
                    }
                    else
                    {
                        strPointID[iCounter, 0] = strPointName;
                        strPointID[iCounter, 1] = "Missing";
                    }

                    dataReader.Close();

                    //Console.WriteLine("TMCLocation.ID:  " + iCounter+" " + strPointID[iCounter, 0]+ "  "  + strPointID[iCounter, 1]);

                    iCounter++;
                    strPointName = strPointNames[iCounter];

                } while (strPointName != "NoMore");

                conn.Dispose();
                conn.Close();
            }

            strPointID[iCounter, 0] = "NoMore";
            strPointID[iCounter, 1] = "0";

            return strPointID;
        }

        public string[,] getMeanDeltasFromDB(string strDBconnection, string strProjectTitle, string strTimeBlockStart, string strTimeBlockEnd, string[,] strSensorID)
        {
            //
            // Purpose:
            //      To extract the mean dN,dE,dH,dR,dT from dbo.TMTPosition_Terrestrial table for the time block strTimeBlockStart to strTimeBlockEnd
            // Input:
            //      Receives 
            //          array of point names & sensorID generated by getSensorIDfromDB() or readPointNamesSensorID(from Reference worksheet)
            //          the Project Title from the config file
            //          the start and end time blocks
            // Output:
            //      Returns array [PointName,dN,dE,dH, dR, dT, number of points used to compute mean]   [0,1,2,3,4,5,6]
            //      strPointDeltas[iCounter, 0] = strPointName;
            //      strPointDeltas[iCounter, 1] = MeandN
            //      strPointDeltas[iCounter, 2] = MeandE
            //      strPointDeltas[iCounter, 3] = MeandH
            //      strPointDeltas[iCounter, 4] = MeandR
            //      strPointDeltas[iCounter, 5] = MeandT
            //      strPointDeltas[iCounter, 6] = ObservationCounter = "-99" id there are no observations
            //
            //
            //
            // Useage:
            //      string[,] strPointDeltas = gna.getPointDeltas(strDBconnection, strProjectTitle, strTimeBlockStart, strTimeBlockEnd, strSensorID);
            // Comment:
            //      If missing then deltas are 0,0,0,-99
            //      last point in list = "NoMore"
            //

            string[,] strDeltas = new string[2000, 7];
            string strPointName;
            string strPointID;
            int iCounter = 0;
            double dbldN;
            double dbldE;
            double dbldH;
            double dbldR;
            double dbldT;
            double dblMeandN = 0.0;
            double dblMeandE = 0.0;
            double dblMeandH = 0.0;
            double dblMeandR = 0.0;
            double dblMeandT = 0.0;
            int iObservationCounter = 0;

            // Select the block of observations for the point within the Time Block: between strRefBlockStart and strRefBlockEnd
            // generate the mean dN, dE, dH, dR, dT

            do
            {
                strPointName = strSensorID[iCounter, 0];
                strPointID = strSensorID[iCounter, 1];


                //Console.WriteLine(strPointName + " " + strPointID);

                //instantiate and open connection
                SqlConnection conn = new(strDBconnection);
                conn.Open();

                if (strPointID == "Missing")
                {
                    goto ComputeMeans;
                }

                // define the SQL query
                string SQLaction = @"
                SELECT * FROM dbo.TMTPosition_Terrestrial  
                WHERE SensorID = @SensorID 
                AND IsOutlier = 0 
                AND EndTimeUTC BETWEEN " + strTimeBlockStart +
                " AND " + strTimeBlockEnd;


                //Console.WriteLine(SQLaction);



                //string strTemp = SQLaction;
                SqlCommand cmd = new(SQLaction, conn);

                // define the parameter used in the command object and add to the command
                cmd.Parameters.Add(new SqlParameter("@SensorID", strPointID));

                // Define the data reader
                SqlDataReader dataReader = cmd.ExecuteReader();

                // Now read through the results and generate a mean value
                dblMeandN = 0.0;
                dblMeandE = 0.0;
                dblMeandH = 0.0;
                dblMeandR = 0.0;
                dblMeandT = 0.0;

                iObservationCounter = 0;

                if (dataReader.HasRows)
                {
                    while (dataReader.Read())
                    {
                        dbldN = Math.Round(Convert.ToDouble(dataReader["dN"]), 4);
                        dbldE = Math.Round(Convert.ToDouble(dataReader["dE"]), 4);
                        dbldH = Math.Round(Convert.ToDouble(dataReader["dH"]), 4);
                        dbldR = Math.Round(Convert.ToDouble(dataReader["dR"]), 4);
                        dbldT = Math.Round(Convert.ToDouble(dataReader["dT"]), 4);

                        //Console.WriteLine(Convert.ToString(dbldN) + "  " + Convert.ToString(dbldE) + "  " + Convert.ToString(dbldH));

                        dblMeandN += dbldN;
                        dblMeandE += dbldE;
                        dblMeandH += dbldH;
                        dblMeandR += dbldR;
                        dblMeandT += dbldT;
                        iObservationCounter++;
                    }
                }
                else
                {
                    dblMeandN = 0.0;
                    dblMeandE = 0.0;
                    dblMeandH = 0.0;
                    dblMeandR = 0.0;
                    dblMeandT = 0.0;
                    iObservationCounter = -99;
                    strSensorID[iCounter, 0] = strPointName;
                    strSensorID[iCounter, 1] = "No Readings";
                    goto NextPoint;
                }

ComputeMeans:

                if ((strPointID != "Missing") && (iObservationCounter > 0))
                {
                    // Compute the mean dN, dE, dH
                    dblMeandN = Math.Round(dblMeandN / iObservationCounter, 4);
                    dblMeandE = Math.Round(dblMeandE / iObservationCounter, 4);
                    dblMeandH = Math.Round(dblMeandH / iObservationCounter, 4);
                    dblMeandR = Math.Round(dblMeandR / iObservationCounter, 4);
                    dblMeandT = Math.Round(dblMeandT / iObservationCounter, 4);
                }
                else
                {
                    // allocate false values
                    dblMeandN = 0.0;
                    dblMeandE = 0.0;
                    dblMeandH = 0.0;
                    dblMeandR = 0.0;
                    dblMeandT = 0.0;
                    iObservationCounter = -99;
                }

                //Console.WriteLine("Mean");
                //Console.WriteLine(Convert.ToString(dblMeandN) + "  " + Convert.ToString(dblMeandE) + "  " + Convert.ToString(dblMeandH));


                //Insert the data into the data arrays
                strDeltas[iCounter, 0] = strPointName;
                strDeltas[iCounter, 1] = Convert.ToString(dblMeandN);
                strDeltas[iCounter, 2] = Convert.ToString(dblMeandE);
                strDeltas[iCounter, 3] = Convert.ToString(dblMeandH);
                strDeltas[iCounter, 4] = Convert.ToString(dblMeandR);
                strDeltas[iCounter, 5] = Convert.ToString(dblMeandT);
                strDeltas[iCounter, 6] = Convert.ToString(iObservationCounter);
NextPoint:

// Close the DB connection
                conn.Dispose();
                conn.Close();

                iCounter++;
                strPointName = strSensorID[iCounter, 0];

            } while (strPointName != "NoMore");

            strDeltas[iCounter, 0] = "NoMore";
            strDeltas[iCounter, 1] = "999";
            strDeltas[iCounter, 2] = "999";
            strDeltas[iCounter, 3] = "999";
            strDeltas[iCounter, 4] = "999";
            strDeltas[iCounter, 5] = "999";
            strDeltas[iCounter, 6] = "0";

            return strDeltas;
        }


        public string getTimeOfLastObservation(string strDBconnection, string strProjectTitle, string strSensorID)
        {
            // Purpose
            //  To return the latest observation UTC time for a single point
            // Input
            //  DB connection, DB project title, Sensor name
            // Output
            //  strLatestObservationUTC= time of latest observation
            //           =  "Project missing" if project is missing
            string strLatestObservationUTC = "";


            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                try
                {
                    // define the SQL query . Do not try MAX()
                    string SQLaction = @"
                    SELECT TOP(1) TMTDistance.EndTimeUTC
                    FROM dbo.TMTDistance 
                    WHERE TMTDistance.SensorID = @SensorID
                    ORDER BY EndTimeUTC DESC";

                    SqlCommand cmd = new(SQLaction, conn);

                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@SensorID", strSensorID));

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    // If the point exists, there will be a result. If not, then the point get marked missing


                    if (!dataReader.Read())
                    {
                        strLatestObservationUTC = "Missing";
                    }
                    else
                    {
                        //strLatestObservationUTC = Convert.ToString((DateTime)dataReader["EndTimeUTC"]);

                        strLatestObservationUTC = ((DateTime)dataReader["EndTimeUTC"]).ToString("dd/MM/yyyy HH:mm:ss");

                    }
                    dataReader.Close();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("getTimeOfLastObservation: SQL selection failed: ");
                    Console.WriteLine(ex);
                    Console.WriteLine("Press key...");
                    Console.ReadKey();
                }
                finally
                {
                    conn.Dispose();
                    conn.Close();
                }
            }
            return strLatestObservationUTC;
        }

        public string getDistanceSensorID(string strDBconnection, string strProjectTitle, string strSensorName)
        {
            // Purpose
            //  To return the SensorID for a single distance
            // Input
            //  DB connection, DB project title, Sensor name
            // Output
            //  SensorID = "Missing" if point is missing
            //           =  "Project missing" if project is missing

            //Console.WriteLine("1 strSensorName "+strSensorName);


            string strSensorID = "";
            Int16 iCounter = 0;

            //Connection and Reader declared outside the try block

            try
            {

                using SqlConnection conn = new(strDBconnection);
                //open connection
                conn.Open();

                // define the SQL query
                string SQLaction = @"
                    SELECT 
                        TMCSensor.ID, 
                        TMCSensor.Name
                    FROM ((TMCSensor
                    INNER JOIN TMCLocation
                        ON TMCSensor.LocationID = TMCLocation.ID)
                    INNER JOIN TMCMonitoringProjects
                        ON TMCLocation.ProjectID = TMCMonitoringProjects.ID
                        AND TMCMonitoringProjects.ProjectTitle = @ProjectName
                        AND TMCMonitoringProjects.IsDeleted = 0
                        )
                    WHERE TMCSensor.Name = @sensorName
                    ";

                SqlCommand cmd = new(SQLaction, conn);

                // define the parameter used in the command object and add to the command
                cmd.Parameters.Add(new SqlParameter("@ProjectName", strProjectTitle));
                cmd.Parameters.Add(new SqlParameter("@sensorName", strSensorName));

                //Console.WriteLine("2 strProjectTitle " + strProjectTitle);

                // Define the data reader
                SqlDataReader dataReader = cmd.ExecuteReader();

                // get the values
                while (dataReader.Read())
                {
                    int iSensorID = (Int32)dataReader["ID"];
                    strSensorID = Convert.ToString(iSensorID);
                    //Console.WriteLine("getDistanceSensorID "+ strSensorID);
                    iCounter++;
                }
                // Close the dataReader
                dataReader?.Close();

                if (iCounter == 0)
                {
                    strSensorID = "Missing";
                    goto Exit;
                }

                conn.Dispose();
                conn.Close();


            }
            catch (SqlException ex)
            {
                Console.WriteLine("getDistanceSensorID: DB connection failed: ");
                Console.WriteLine(ex);
                Console.WriteLine("Press key...");
                Console.ReadKey();
            }

Exit:

            return strSensorID;
        }


        public string getSinglePointSensorID(string strDBconnection, string strProjectTitle, string strSensorName)
        {
            // Purpose
            //  To return the SensorID for a single distance
            // Input
            //  DB connection, DB project title, Sensor name
            // Output
            //  SensorID = "Missing" if point is missing
            //           =  "Project missing" if project is missing

            //Console.WriteLine("1 strSensorName "+strSensorName);


            string strSensorID = "";
            Int16 iCounter = 0;

            // Connection and Reader declared outside the try block
            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();

                try
                {

                    // define the SQL query
                    string SQLaction = @"
                    SELECT 
                        TMCSensor.ID, 
                        TMCSensor.Name
                    FROM ((TMCSensor
                    INNER JOIN TMCLocation
                        ON TMCSensor.LocationID = TMCLocation.ID)
                    INNER JOIN TMCMonitoringProjects
                        ON TMCLocation.ProjectID = TMCMonitoringProjects.ID
                        AND TMCMonitoringProjects.ProjectTitle = @ProjectName
                        AND TMCMonitoringProjects.IsDeleted = 0
                        )
                    WHERE TMCSensor.Name = @sensorName
                    ";

                    SqlCommand cmd = new(SQLaction, conn);

                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@ProjectName", strProjectTitle));
                    cmd.Parameters.Add(new SqlParameter("@sensorName", strSensorName));

                    //Console.WriteLine("2 strProjectTitle " + strProjectTitle);

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    while (dataReader.Read())
                    {
                        int iSensorID = (Int32)dataReader["ID"];
                        strSensorID = Convert.ToString(iSensorID);
                        //Console.WriteLine("getDistanceSensorID "+ strSensorID);
                        iCounter++;
                    }
                    // Close the dataReader
                    dataReader?.Close();
                    if (iCounter == 0)
                    {
                        strSensorID = "Missing";
                        goto Exit;
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("getDistanceSensorID: DB connection failed: ");
                    Console.WriteLine(ex);
                    Console.WriteLine("Press key...");
                    Console.ReadKey();
                }
                finally
                {
                    conn.Dispose();
                    conn.Close();
                }
            }

Exit:

            return strSensorID;
        }


        public string[,] getSensorIDfromDB(string strDBconnection, string[] strPointNames, string strProjectName)
        {

            //  20221015: NOTE This is a completely revised version of the getSensorID in gnaLibrary
            //              I take into account that the Location name may not match the sensor name
            //  
            //
            // Purpose:
            //      To extract the sensor ID from TMCSensor table
            // Input:
            //      Receives array of strNamesID[point names, locationID] from getLocationID(), and the project title
            // Output:
            //      Returns array [PointName,SensorID]
            // Useage:
            //      string[,] strSensorID = gna.getSensorID(strDBconnection, strProjectTitle, strNamesID);
            // Comment:
            //      If missing then ID="Missing"
            //      last point in list = "NoMore"
            //



            string[,] strSensorID = new string[5000, 2];
            string strPointName;
            _ = new int[100];
            int iCounter = 0;

            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                do
                {



                    // define the SQL query
                    string SQLaction = @"
                    SELECT 
                        TMCSensor.ID, 
                        TMCSensor.Name
                    FROM ((TMCSensor
                    INNER JOIN TMCLocation
                        ON TMCSensor.LocationID = TMCLocation.ID)
                    INNER JOIN TMCMonitoringProjects
                        ON TMCLocation.ProjectID = TMCMonitoringProjects.ID
                        AND TMCMonitoringProjects.ProjectTitle = @ProjectName
                        AND TMCMonitoringProjects.IsDeleted = 0
                        )
                    WHERE TMCSensor.Name = @sensorName
                    AND TMCSensor.IsEnabled = 1
                    AND TMCSensor.IsDeleted = 0
                    ";

                    SqlCommand cmd = new(SQLaction, conn);

                    strPointName = strPointNames[iCounter];

                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@sensorName", strPointName));
                    cmd.Parameters.Add(new SqlParameter("@ProjectName", strProjectName));

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    // If the point exists, there will be a result. If not, then the point get marked missing

                    if (dataReader.HasRows)
                    {
                        while (dataReader.Read())
                        {
                            int iSensorID = (Int32)dataReader["ID"];
                            strSensorID[iCounter, 0] = strPointName;
                            strSensorID[iCounter, 1] = Convert.ToString(iSensorID);
                        }
                    }
                    else
                    {
                        strSensorID[iCounter, 0] = strPointName;
                        strSensorID[iCounter, 1] = "Missing";
                    }

                    dataReader.Close();


                    //Console.WriteLine("TMCSensor.ID:  " + iCounter+" " + strSensorID[iCounter, 0]+ "  "  + strSensorID[iCounter, 1]);

                    iCounter++;
                    strPointName = strPointNames[iCounter];

                } while (strPointName != "NoMore");

                conn.Dispose();
                conn.Close();
            }

            strSensorID[iCounter, 0] = "NoMore";
            strSensorID[iCounter, 1] = "0";

            return strSensorID;
        }

        public string[,] getLatestDeltasFromDB(string strDBconnection, string strProjectTitle, string strTimeBlockStart, string strTimeBlockEnd, string[,] strSensorID)
        {
            //
            // Purpose:
            //      To extract the mean dN,dE,dH,dR,dT from dbo.TMTPosition_Terrestrial table for the time block strTimeBlockStart to strTimeBlockEnd
            // Input:
            //      Receives 
            //          array of point names & sensorID generated by getSensorIDfromDB() or readPointNamesSensorID(from Reference worksheet)
            //          the Project Title from the config file
            //          the start and end time blocks
            // Output:
            //      Returns array [PointName,dN,dE,dH, dR, dT, number of points used to compute mean]   [0,1,2,3,4,5,6]
            //      strPointDeltas[iCounter, 0] = strPointName;
            //      strPointDeltas[iCounter, 1] = latestdN
            //      strPointDeltas[iCounter, 2] = latestdE
            //      strPointDeltas[iCounter, 3] = latestdH
            //      strPointDeltas[iCounter, 4] = blank
            //      strPointDeltas[iCounter, 5] = latestObservationTime
            //      strPointDeltas[iCounter, 6] = ObservationCounter = "-99" id there are no observations
            //
            //
            //
            // Useage:
            //      string[,] strPointDeltas = gna.getPointDeltas(strDBconnection, strProjectTitle, strTimeBlockStart, strTimeBlockEnd, strSensorID);
            // Comment:
            //      If missing then deltas are 0,0,0,-99
            //      last point in list = "NoMore"
            //

            string[,] strDeltas = new string[2000, 7];
            string strPointName;
            string strPointID;
            int iCounter = 0;
            double dblLatestdN = 0.0;
            double dblLatestdE = 0.0;
            double dblLatestdH = 0.0;
            int iObservationCounter;
            string strLatestObservationUTC;

            // Select the block of observations for the point within the Time Block: between strRefBlockStart and strRefBlockEnd
            // generate the mean dN, dE, dH, dR, dT

            do
            {
                strPointName = strSensorID[iCounter, 0];
                strPointID = strSensorID[iCounter, 1];


                //Console.WriteLine(strPointName + " " + strPointID);

                //instantiate and open connection
                SqlConnection conn = new(strDBconnection);
                conn.Open();

                if (strPointID == "Missing")
                {
                    strLatestObservationUTC = "Missing";
                    iObservationCounter = 0;
                    goto PrepareData;

                }
                // define the SQL query
                string SQLaction = @"
                SELECT * FROM dbo.TMTPosition_Terrestrial  
                WHERE SensorID = @SensorID 
                AND IsOutlier = 0 
                AND EndTimeUTC BETWEEN " + strTimeBlockStart +
                " AND " + strTimeBlockEnd +
                " ORDER BY EndTimeUTC DESC";

                //string strTemp = SQLaction;
                SqlCommand cmd = new(SQLaction, conn);

                // define the parameter used in the command object and add to the command
                cmd.Parameters.Add(new SqlParameter("@SensorID", strPointID));

                // Define the data reader
                SqlDataReader dataReader = cmd.ExecuteReader();



                if (!dataReader.Read())
                {
                    strLatestObservationUTC = "Missing";
                    iObservationCounter = 0;
                    goto PrepareData;
                }
                else
                {
                    //strLatestObservationUTC = ((DateTime)dataReader["EndTimeUTC"]).ToString("dd/MM/yyyy HH:mm:ss");
                    dblLatestdN = Math.Round(Convert.ToDouble(dataReader["dN"]), 4);
                    dblLatestdE = Math.Round(Convert.ToDouble(dataReader["dE"]), 4);
                    dblLatestdH = Math.Round(Convert.ToDouble(dataReader["dH"]), 4);
                    strLatestObservationUTC = ((DateTime)dataReader["EndTimeUTC"]).ToString("yyyy-MM-dd HH:mm");
                    iObservationCounter = 1;
                }

                dataReader.Close();

PrepareData:

//Insert the data into the data arrays
                strDeltas[iCounter, 0] = strPointName;
                strDeltas[iCounter, 1] = Convert.ToString(dblLatestdN);
                strDeltas[iCounter, 2] = Convert.ToString(dblLatestdE);
                strDeltas[iCounter, 3] = Convert.ToString(dblLatestdH);
                strDeltas[iCounter, 4] = "blank";
                strDeltas[iCounter, 5] = strLatestObservationUTC;
                strDeltas[iCounter, 6] = Convert.ToString(iObservationCounter);


                //Console.WriteLine(strDeltas[iCounter, 0]+"  "+ strDeltas[iCounter, 5] + "  " + strDeltas[iCounter, 6]);




                // Close the DB connection
                conn.Dispose();
                conn.Close();

                iCounter++;
                strPointName = strSensorID[iCounter, 0];

            } while (strPointName != "NoMore");

            strDeltas[iCounter, 0] = "NoMore";
            strDeltas[iCounter, 1] = "999";
            strDeltas[iCounter, 2] = "999";
            strDeltas[iCounter, 3] = "999";
            strDeltas[iCounter, 4] = "999";
            strDeltas[iCounter, 5] = "999";
            strDeltas[iCounter, 6] = "0";

            return strDeltas;
        }


        public string[,] getSensorIDfromdataAnalysisDB(string strDBconnection, string strpointNamesTable)
        {

            //
            // Purpose:
            //      To extract the sensor ID, plus names and rail bracket from dataAnalysis.pointNames table
            // Input:
            //      DB connection
            // Output:
            //      Returns array [ID,shortName,longName, railBracket]
            // Useage:
            //      string[,] strSensorID = gnaDBAPI.getSensorIDfromdataAnalysisDB(strDBconnection, strpointNamesTable);
            // Comment:
            //      If missing then ID="Missing"
            //      last point in list = "NoMore"
            //

            string[,] strSensorID = new string[5000, 4];

            _ = new int[100];
            int iCounter = 0;

            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();

                // define the SQL query
                string SQLaction = @"
                    SELECT 
                        pointNames.ID, 
                        pointNames.shortName,
                        pointNames.longName,
                        pointNames.railBracket
                    FROM dbo.pointNames
                    ";

                SqlCommand cmd = new(SQLaction, conn);


                // Define the data reader
                SqlDataReader dataReader = cmd.ExecuteReader();

                // get the values

                if (dataReader.HasRows)
                {
                    while (dataReader.Read())
                    {
                        strSensorID[iCounter, 0] = Convert.ToString((Int32)dataReader["ID"]);
                        strSensorID[iCounter, 1] = (string)dataReader["shortName"];
                        strSensorID[iCounter, 2] = (string)dataReader["longName"];
                        strSensorID[iCounter, 3] = (string)dataReader["railBracket"];
                        iCounter++;
                    }
                }

                dataReader.Close();

                conn.Dispose();
                conn.Close();
            }

            strSensorID[iCounter, 0] = "NoMore";
            strSensorID[iCounter, 1] = "0";

            return strSensorID;
        }





        //====[Put methods]============

        public void putFixedData(string strDBconnection, List<FixedData> fixedData, string strfixedDataTable)
        {
            // Purpose:
            //      To write the fixed data to the dataAlalysis>fixedData table
            // Input:
            //      array[pointName,replacementName,railBracket, Esurvey,Nsurvey,Hsurvey]
            // Output:
            //      None
            // Useage:
            //     gna.putFixedData(strDBconnection, strBothPointNames, strpointNamesTable);
            //

            string strName = "";
            int i = 0;

            //do
            //{
            //    Console.WriteLine(fixedData[i].shortName + "  " + fixedData[i].Esurvey + "  " + fixedData[i].Nsurvey + "  " + fixedData[i].Hsurvey + "  " + fixedData[i].railBracket);
            //    i++;
            //    strName = fixedData[i].shortName;

            //} while (strName != "NoMore");

            //Console.WriteLine("\ndatabaseAPI..");
            //Console.ReadKey();

            // Connection and Reader declared outside the try block
            using SqlConnection conn = new(strDBconnection);

            //instantiate and open connection
            conn.Open();
            try
            {
                do
                {
                    string strID = i.ToString();
                    string SQLaction =
                        "INSERT INTO " + strfixedDataTable + " (ID, shortName, longName, railBracket, Esurvey, Nsurvey, Hsurvey) " + "Values ('" + strID + "','" + fixedData[i].shortName + "','" + fixedData[i].longName + "','" + fixedData[i].railBracket + "','" + fixedData[i].Esurvey.ToString() + "','" + fixedData[i].Nsurvey.ToString() + "','" + fixedData[i].Hsurvey.ToString() + "')";

                    SqlCommand cmd = new(SQLaction, conn);
                    // define the parameter used in the command object and add to the command
                    cmd.ExecuteNonQuery();
                    i++;
                    strName = fixedData[i].shortName;
                } while (strName != "NoMore");
            }

            catch (System.Data.SqlClient.SqlException ex)

            {
                Console.WriteLine("putBothPointNames: DB connection failed : ");
                Console.WriteLine(ex);
                Console.ReadKey();
            }

            finally
            {
                conn.Dispose();
                conn.Close();
            }


        }





        //===[ Geomos methods ]=====

        public string getGeomosProjectID(string strDBconnection, string strMonitoringSystemsName)
        {
            // Purpose:
            //      To determine the project ID from MonitoringSystems
            // Input:
            //      Receives Project Title
            // Output:
            //      Returns Project ID
            // Useage:
            //      string strProjectID = gnaDBAPI.getGeomosProjectID(strDBconnection, strProjectTitle);
            //

            string strProjectID = "";
            Int16 iCounter = 0;

            // Connection and Reader declared outside the try block
            using (SqlConnection conn = new(strDBconnection))
            {

                //instantiate and open connection
                conn.Open();

                try
                {
                    // define the SQL query
                    string SQLaction = @"
                    SELECT ID, Name  
                    FROM dbo.MonitoringSystems 
                    WHERE Name = @MonitoringSystemsName
                    AND Active = 0
                    ";
                    SqlCommand cmd = new(SQLaction, conn);

                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@MonitoringSystemsName", @strMonitoringSystemsName));

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    while (dataReader.Read())
                    {
                        int iProjectID = (Int32)dataReader["ID"];
                        strProjectID = Convert.ToString(iProjectID);
                        iCounter++;
                    }


                    // Close the dataReader
                    dataReader?.Close();

                }

                catch (System.Data.SqlClient.SqlException ex)
                {
                    Console.WriteLine("getGeomosProjectID: DB Connection Failed when retrieving Project ID => Project name not correct : ");
                    Console.WriteLine(ex);
                    Console.ReadKey();
                }

                finally
                {
                    conn.Dispose();
                    conn.Close();
                }
            }

            if (iCounter == 0) { strProjectID = "Missing"; }

            return strProjectID;
        }

        public string[,] getGeomosSensorID(string strDBconnection, string[] strPointNames, string strMonitoringSystemsName)
        {


            //
            // Purpose:
            //      To extract the point ID from Points table
            // Input:
            //      Receives array of strPointNames[point names] from readPointNames(), and the project title
            // Output:
            //      Returns array [PointName,SensorID]
            // Useage:
            //      string[,] strSensorID = gna.getGeomosSensorID(strDBconnection, strProjectTitle, strNamesID);
            // Comment:
            //      If missing then ID="Missing"
            //      last point in list = "NoMore"
            //



            string[,] strSensorID = new string[5000, 2];
            string strPointName;
            _ = new int[100];
            int iCounter = 0;

            // get the project ID
            string strMonitoringSysID = getGeomosProjectID(strDBconnection, strMonitoringSystemsName);


            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                do
                {
                    // define the SQL query
                    string SQLaction = @"
                    SELECT ID, Name  
                    FROM dbo.Points 
                    WHERE Points.Name = @sensorName
                    AND Points.MonitoringSys_ID = @MonitoringSysID
                    AND Points.Role = 0
                    ";

                    SqlCommand cmd = new(SQLaction, conn);

                    strPointName = strPointNames[iCounter];

                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@sensorName", strPointName));
                    cmd.Parameters.Add(new SqlParameter("@MonitoringSysID", strMonitoringSysID));

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    // If the point exists, there will be a result. If not, then the point get marked missing

                    if (dataReader.HasRows)
                    {
                        while (dataReader.Read())
                        {
                            int iSensorID = (Int32)dataReader["ID"];
                            strSensorID[iCounter, 0] = strPointName;
                            strSensorID[iCounter, 1] = Convert.ToString(iSensorID);
                        }
                    }
                    else
                    {
                        strSensorID[iCounter, 0] = strPointName;
                        strSensorID[iCounter, 1] = "Missing";
                    }

                    dataReader.Close();


                    //Console.WriteLine("GeoMoS.Points.ID:  " + strSensorID[iCounter, 0]+ "  "  + strSensorID[iCounter, 1]);

                    iCounter++;
                    strPointName = strPointNames[iCounter];

                } while (strPointName != "NoMore");

                conn.Dispose();
                conn.Close();
            }

            strSensorID[iCounter, 0] = "NoMore";
            strSensorID[iCounter, 1] = "0";

            return strSensorID;
        }

        public string[,] getGeomosMeanDeltas(string strDBconnection, string strProjectTitle, string strTimeBlockStart, string strTimeBlockEnd, string[,] strSensorID)
        {
            //
            // Purpose:
            //      To extract the mean dN,dE,dH,dR,dT from dbo.Results table for the time block strTimeBlockStart to strTimeBlockEnd
            // Input:
            //      Receives 
            //          array of point names & sensorID generated by getSensorIDfromDB() or readPointNamesSensorID(from Reference worksheet)
            //          the Project Title from the config file
            //          the start and end time blocks
            // Output:
            //      Returns array [PointName,dN,dE,dH, dR, dT, number of points used to compute mean]   [0,1,2,3,4,5,6]
            //      strPointDeltas[iCounter, 0] = strPointName;
            //      strPointDeltas[iCounter, 1] = MeandN
            //      strPointDeltas[iCounter, 2] = MeandE
            //      strPointDeltas[iCounter, 3] = MeandH
            //      strPointDeltas[iCounter, 4] = MeandR
            //      strPointDeltas[iCounter, 5] = MeandT
            //      strPointDeltas[iCounter, 6] = ObservationCounter = "-99" id there are no observations
            //
            //
            //
            // Useage:
            //      string[,] strPointDeltas = gna.getPointDeltas(strDBconnection, strProjectTitle, strTimeBlockStart, strTimeBlockEnd, strSensorID);
            // Comment:
            //      If missing then deltas are 0,0,0,-99
            //      last point in list = "NoMore"
            //

            string[,] strDeltas = new string[2000, 7];
            string strPointName;
            string strPointID;
            int iCounter = 0;
            double dbldN;
            double dbldE;
            double dbldH;
            double dbldR;
            double dbldT;
            double dblMeandN = 0.0;
            double dblMeandE = 0.0;
            double dblMeandH = 0.0;
            double dblMeandR = 0.0;
            double dblMeandT = 0.0;
            int iObservationCounter = 0;

            // Select the block of observations for the point within the Time Block: between strRefBlockStart and strRefBlockEnd
            // generate the mean dN, dE, dH, dR, dT

            do
            {
                strPointName = strSensorID[iCounter, 0];
                strPointID = strSensorID[iCounter, 1];


                //Console.WriteLine(strPointName + " " + strPointID);

                //instantiate and open connection
                SqlConnection conn = new(strDBconnection);
                conn.Open();

                if (strPointID == "Missing")
                {
                    goto ComputeMeans;
                }

                // define the SQL query
                string SQLaction = @"
                SELECT * FROM dbo.Results 
                WHERE Results.Point_ID = @PointID 
                AND Results.Type = 0 
                AND EPOCH BETWEEN " + strTimeBlockStart +
                " AND " + strTimeBlockEnd;

                //string strTemp = SQLaction;
                SqlCommand cmd = new(SQLaction, conn);

                // define the parameter used in the command object and add to the command
                cmd.Parameters.Add(new SqlParameter("@PointID", strPointID));

                // Define the data reader
                SqlDataReader dataReader = cmd.ExecuteReader();

                // Now read through the results and generate a mean value
                dblMeandN = 0.0;
                dblMeandE = 0.0;
                dblMeandH = 0.0;
                dblMeandR = 0.0;
                dblMeandT = 0.0;

                iObservationCounter = 0;

                if (dataReader.HasRows)
                {
                    while (dataReader.Read())
                    {
                        dbldN = Math.Round(Convert.ToDouble(dataReader["NorthingDiff"]), 4);
                        dbldE = Math.Round(Convert.ToDouble(dataReader["EastingDiff"]), 4);
                        dbldH = Math.Round(Convert.ToDouble(dataReader["HeightDiff"]), 4);
                        dbldR = Math.Round(Convert.ToDouble(dataReader["LongitudinalDisplacement"]), 4);
                        dbldT = Math.Round(Convert.ToDouble(dataReader["TransverseDisplacement"]), 4);

                        //Console.WriteLine(Convert.ToString(dbldN) + "  " + Convert.ToString(dbldE) + "  " + Convert.ToString(dbldH));

                        dblMeandN += dbldN;
                        dblMeandE += dbldE;
                        dblMeandH += dbldH;
                        dblMeandR += dbldR;
                        dblMeandT += dbldT;
                        iObservationCounter++;
                    }
                }
                else
                {
                    dblMeandN = 0.0;
                    dblMeandE = 0.0;
                    dblMeandH = 0.0;
                    dblMeandR = 0.0;
                    dblMeandT = 0.0;
                    iObservationCounter = -99;
                    strSensorID[iCounter, 0] = strPointName;
                    strSensorID[iCounter, 1] = "No Readings";
                    goto NextPoint;
                }

ComputeMeans:

                if ((strPointID != "Missing") && (iObservationCounter > 0))
                {
                    // Compute the mean dN, dE, dH
                    dblMeandN = Math.Round(dblMeandN / iObservationCounter, 4);
                    dblMeandE = Math.Round(dblMeandE / iObservationCounter, 4);
                    dblMeandH = Math.Round(dblMeandH / iObservationCounter, 4);
                    dblMeandR = Math.Round(dblMeandR / iObservationCounter, 4);
                    dblMeandT = Math.Round(dblMeandT / iObservationCounter, 4);
                }
                else
                {
                    // allocate false values
                    dblMeandN = 0.0;
                    dblMeandE = 0.0;
                    dblMeandH = 0.0;
                    dblMeandR = 0.0;
                    dblMeandT = 0.0;
                    iObservationCounter = -99;
                }

                //Console.WriteLine("Mean");
                //Console.WriteLine(Convert.ToString(dblMeandN) + "  " + Convert.ToString(dblMeandE) + "  " + Convert.ToString(dblMeandH));


                //Insert the data into the data arrays
                strDeltas[iCounter, 0] = strPointName;
                strDeltas[iCounter, 1] = Convert.ToString(dblMeandN);
                strDeltas[iCounter, 2] = Convert.ToString(dblMeandE);
                strDeltas[iCounter, 3] = Convert.ToString(dblMeandH);
                strDeltas[iCounter, 4] = Convert.ToString(dblMeandR);
                strDeltas[iCounter, 5] = Convert.ToString(dblMeandT);
                strDeltas[iCounter, 6] = Convert.ToString(iObservationCounter);
NextPoint:

// Close the DB connection
                conn.Dispose();
                conn.Close();

                iCounter++;
                strPointName = strSensorID[iCounter, 0];

            } while (strPointName != "NoMore");

            strDeltas[iCounter, 0] = "NoMore";
            strDeltas[iCounter, 1] = "999";
            strDeltas[iCounter, 2] = "999";
            strDeltas[iCounter, 3] = "999";
            strDeltas[iCounter, 4] = "999";
            strDeltas[iCounter, 5] = "999";
            strDeltas[iCounter, 6] = "0";

            return strDeltas;
        }

    }
}
