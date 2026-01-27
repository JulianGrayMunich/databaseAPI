using System.Data;
using System.Globalization;
using GNAsurveytools;
using gnaDataClasses;
using Microsoft.Data.SqlClient;







//
//  20240509    Add getAlarmStatus
//


//===============[Initial settings]======================================
#pragma warning disable CS0618
#pragma warning disable CS8600
#pragma warning disable IDE1006
#pragma warning disable NU1510


namespace databaseAPI
{

    public class dbAPI
    {
        GNAsurveycalcs gnaSurvey = new();
        //gnaTools gnaT = new();

        string strTab1 = "     ";
        string strTab2 = "        ";
        public void testDBconnection(string connectionString)
        {

            try
            {
                using var conn = new SqlConnection(connectionString);
                conn.Open();
                Console.WriteLine(strTab2 + "DB Connection Successful");
            }
            catch (SqlException ex)
            {
                Console.WriteLine(strTab2 + $"DB connection failed:\n        {ex.Message}\n");
            }

        }

        //=== working

        public string LocalToUTC(string localTime)
        {

            try
            {
                // Normalize input: handle null/whitespace, strip single/double quotes, trim
                localTime = string.IsNullOrWhiteSpace(localTime)
                    ? string.Empty
                    : localTime.Replace("'", string.Empty)
                               .Replace("\"", string.Empty)
                               .Trim();

                if (string.IsNullOrEmpty(localTime))
                    throw new ArgumentException("Input timestamp is null or empty.", nameof(localTime));

                // Accept both with and without seconds
                string[] formats = { "yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss" };
                var culture = CultureInfo.InvariantCulture;

                if (!DateTime.TryParseExact(localTime, formats, culture, DateTimeStyles.AssumeLocal, out DateTime localDateTime))
                    throw new FormatException($"Invalid local time format or impossible date: '{localTime}'. Expected yyyy-MM-dd HH:mm[:ss].");

                // Convert to UTC and return with seconds, no quotes
                DateTime utcDateTime = localDateTime.ToUniversalTime();
                return utcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }
            catch (Exception ex)
            {
                // Echo error to console before rethrow
                Console.WriteLine($"convertLocalToUTC failed. Input='{localTime}'.\nError={ex.Message}");
                throw; // preserve original stack trace
            }
        }


        public List<PrismTimeBlockObservationCount> extractPrismStats(
            string strDBconnection,
            List<PrismIdentity> prismIdentities,
            List<Tuple<string, string>> subBlocks)
        {
            if (string.IsNullOrWhiteSpace(strDBconnection))
                throw new ArgumentException("DB connection string not provided.", nameof(strDBconnection));

            if (prismIdentities == null)
                throw new ArgumentNullException(nameof(prismIdentities));

            if (subBlocks == null)
                throw new ArgumentNullException(nameof(subBlocks));

            var results = new List<PrismTimeBlockObservationCount>(
                prismIdentities.Count * Math.Max(subBlocks.Count, 1));

            // If there are no blocks, return empty deterministically.
            if (subBlocks.Count == 0 || prismIdentities.Count == 0)
                return results;

            // SQL semantics: match legacy BETWEEN inclusive behaviour.
            // Use EndTimeUTC >= @StartUtc AND EndTimeUTC <= @EndUtc (equivalent and parameter-friendly).
            const string sql = @"
                SELECT COUNT_BIG(1)
                FROM dbo.TMTPosition_Terrestrial
                WHERE SensorID = @SensorID
                AND IsOutlier = 0
                AND EndTimeUTC >= @StartUtc
                AND EndTimeUTC <= @EndUtc;";

            var utcFormat = "yyyy-MM-dd HH:mm:ss";
            var culture = CultureInfo.InvariantCulture;

            using (var conn = new SqlConnection(strDBconnection))
            {
                conn.Open();

                using (var cmd = new SqlCommand(sql, conn))
                {
                    // Prepare parameters once and reuse.
                    var pSensorId = cmd.Parameters.Add("@SensorID", System.Data.SqlDbType.VarChar, 50);
                    var pStartUtc = cmd.Parameters.Add("@StartUtc", System.Data.SqlDbType.DateTime2);
                    var pEndUtc = cmd.Parameters.Add("@EndUtc", System.Data.SqlDbType.DateTime2);

                    for (int blockIndex = 0; blockIndex < subBlocks.Count; blockIndex++)
                    {
                        var block = subBlocks[blockIndex];

                        if (block == null)
                            throw new InvalidOperationException($"subBlocks[{blockIndex}] is null.");

                        //string localStart = block.Item1 ?? string.Empty;
                        //string localEnd = block.Item2 ?? string.Empty;

                        //Convert local -> UTC using your canonical method.
                        //convertLocalToUTC returns "yyyy-MM-dd HH:mm:ss"
                        //string utcStartString = LocalToUTC(localStart);
                        //string utcEndString = LocalToUTC(localEnd);


                        string utcStartString = block.Item1 ?? string.Empty;
                        string utcEndString = block.Item2 ?? string.Empty;




                        if (!DateTime.TryParseExact(utcStartString, utcFormat, culture,
                                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                                out DateTime utcStart))
                        {
                            throw new FormatException($"Invalid UTC start time: '{utcStartString}' (block {blockIndex}).");
                        }

                        if (!DateTime.TryParseExact(utcEndString, utcFormat, culture,
                                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                                out DateTime utcEnd))
                        {
                            throw new FormatException($"Invalid UTC end time: '{utcEndString}' (block {blockIndex}).");
                        }

                        if (utcEnd < utcStart)
                            throw new InvalidOperationException($"Time block end precedes start (block {blockIndex}): {utcStartString} -> {utcEndString}");

                        // Set time parameters once per block.
                        pStartUtc.Value = utcStart;
                        pEndUtc.Value = utcEnd;

                        for (int i = 0; i < prismIdentities.Count; i++)
                        {
                            PrismIdentity prism = prismIdentities[i];

                            string sensorId = prism.SensorId?.Trim() ?? string.Empty;

                            int count = 0;

                            if (!string.IsNullOrEmpty(sensorId))
                            {
                                pSensorId.Value = sensorId;

                                object? scalar = cmd.ExecuteScalar();

                                // COUNT_BIG returns long
                                long raw = 0;
                                if (scalar != null && scalar != DBNull.Value)
                                    raw = Convert.ToInt64(scalar, CultureInfo.InvariantCulture);

                                if (raw < 0) raw = 0;
                                if (raw > int.MaxValue) raw = int.MaxValue; // deterministic clamp

                                count = (int)raw;
                            }

                            results.Add(new PrismTimeBlockObservationCount
                            {
                                SensorId = sensorId,
                                PrismName = prism.PrismName ?? string.Empty,
                                ReplacementName = prism.ReplacementName,
                                AtsName = prism.AtsName ?? string.Empty,
                                TimeBlockIndex = blockIndex,
                                BlockStartUtc = utcStart,
                                BlockEndUtc = utcEnd,
                                ObservationCount = count
                            });
                        }
                    }
                }
            }

            return results;
        }










        //====[Maintenance methods]===


        public List<SensorInfo> retrieveIsSensorRead(string strDBconnection, List<SensorInfo> sensorList, string strTimeBlockStartUTC, string strTimeBlockEndUTC)
        {
            try
            {
                using (var conn = new SqlConnection(strDBconnection))
                {
                    conn.Open();

                    foreach (var sensor in sensorList)
                    {
                        int readCount = 0;

                        // SQL statement (kept intact as instructed)
                        string SQLaction = @"
SELECT * FROM dbo.TMTPosition_Terrestrial  
WHERE SensorID = @SensorID 
AND IsOutlier = 0 
AND EndTimeUTC BETWEEN " + strTimeBlockStartUTC + " AND " + strTimeBlockEndUTC;

                        using (var cmd = new SqlCommand(SQLaction, conn))
                        {
                            cmd.Parameters.Add(new SqlParameter("@SensorID", sensor.SensorID));

                            using (var dataReader = cmd.ExecuteReader())
                            {
                                while (dataReader.Read())
                                {
                                    readCount++;
                                }
                            }
                        }

                        // Update the Read property
                        sensor.Read = (readCount > 0) ? "Yes" : "No";
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in retrieveIsSensorRead: " + ex.Message);
            }

            return sensorList;
        }



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

            catch (SqlException ex)
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


        public List<Observation> getMeanCoordinates(string strDBconnection, string strPrismName, double Eprev, double Nprev, double Hprev, double Eref, double Nref, double Href, string strStart, string strTimeBlockStart, string strBlockSizeDays, string strLastBlockEndDate)
        {

            // outlier filter
            double dblOutlierFilter = 0.020;

            string strPreviousDate = "";
            string strPreviousMeanDate = "";
            DateTime dtTimeBlockStart, dtTimeBlockEnd, dtPrismDate;

            List<Observation> meanPrismObs = new();
            List<Observation> prismObs = new();

            using (SqlConnection conn = new(strDBconnection))
            {
                conn.Open();
                try
                {
                    string SQLaction = @"
                    SELECT * 
                    FROM Observations
                    WHERE Name = @Name
                    ORDER BY EndTimeUTC";

                    SqlCommand cmd = new(SQLaction, conn);
                    cmd.Parameters.Add(new SqlParameter("@Name", strPrismName));
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    int iIndex = 0;
                    string strCurrentTime = "";
                    string strPreviousTime = "2000-01-01 00:00:00";

                    // Write the observations to the prismObs list 
                    while (dataReader.Read())
                    {

                        // Removes duplicates and outliers
                        strCurrentTime = ((DateTime)dataReader["EndTimeUTC"]).ToString("yyyy-MM-dd HH:mm:ss"); // 24-hour format


                        if (strCurrentTime != strPreviousTime)
                        {
                            double dblE = Math.Round((double)dataReader["E"], 4);
                            double dblN = Math.Round((double)dataReader["N"], 4);
                            double dblH = Math.Round((double)dataReader["H"], 4);
                            double dE = Math.Abs(dblE - Eref);
                            double dN = Math.Abs(dblN - Nref);
                            double dH = Math.Abs(dblH - Href);


                            if ((dE < dblOutlierFilter) && (dN < dblOutlierFilter) && (dH < dblOutlierFilter))
                            {
                                prismObs.Add(new Observation()
                                {
                                    Name = strPrismName,
                                    railBracket = (string)dataReader["railBracket"],
                                    E = dblE,
                                    N = dblN,
                                    H = dblH,
                                    UTCtime = ((DateTime)dataReader["EndTimeUTC"]).ToString("yyyy-MM-dd 00:00:00")
                                });
                            }

                        }
                        strPreviousTime = strCurrentTime;
                        iIndex++;
                    }
                    dataReader?.Close();

                    // Verify that there were readings

                    if (prismObs.Count < 1)
                    {
                        Console.WriteLine("          " + strPrismName + " (EMPTY NO OBS)");
                        meanPrismObs.Add(new Observation()
                        {
                            Name = "Empty"
                        });
                        conn.Close();
                        conn.Dispose();
                        goto ExitPoint;
                    }
                    else
                    {
                        Console.WriteLine("          " + strPrismName + " " + prismObs.Count + " obs");
                    }





                    //double dblTimeBlockSizeDays = Math.Round(Convert.ToDouble(strBlockSizeDays) - 0.9, 1);
                    double dblTimeBlockSizeDays = Math.Round(Convert.ToDouble(strBlockSizeDays) - 0.000694, 1); // reduce by 1 minute
                    dtTimeBlockStart = DateTime.Parse(strTimeBlockStart, CultureInfo.InvariantCulture);
                    dtTimeBlockEnd = dtTimeBlockStart.AddDays(dblTimeBlockSizeDays + 0.000694); // add the minute back
                    string strTimeBlockEnd = dtTimeBlockEnd.ToString("yyyy-MM-dd 23:59:59");
                    dtTimeBlockEnd = DateTime.Parse(strTimeBlockEnd, CultureInfo.InvariantCulture);

                    string strMeanDate = strTimeBlockStart;
                    strPreviousDate = strTimeBlockStart;
                    string strRailBracket = "";

                    int iNoOfObs = prismObs.Count;
                    int iObsCounter = 0;

                    // Reset the summation and mean variables
                    double Esum = 0.0, Nsum = 0.0, Hsum = 0.0;
                    double Emean = 0.0, Nmean = 0.0, Hmean = 0.0;
                    int iBlockCounter = 0;
                    string strComputedMeanFlag = "No";



                    // Compute the means for each time block
                    do
                    {
                        string strPrismDate = prismObs[iObsCounter].UTCtime;
                        strRailBracket = prismObs[iObsCounter].railBracket;

                        dtPrismDate = DateTime.Parse(strPrismDate, CultureInfo.InvariantCulture);

                        if ((dtPrismDate >= dtTimeBlockStart) && (dtPrismDate < dtTimeBlockEnd))
                        {
                            iBlockCounter++;
                            Emean += prismObs[iObsCounter].E;
                            Nmean += prismObs[iObsCounter].N;
                            Hmean += prismObs[iObsCounter].H;
                        }
                        else if (dtPrismDate > dtTimeBlockEnd)
                        {
                            strComputedMeanFlag = "Yes";
                            if (iBlockCounter > 0)
                            {
                                Emean = Math.Round((Emean / iBlockCounter), 4);
                                Nmean = Math.Round((Nmean / iBlockCounter), 4);
                                Hmean = Math.Round((Hmean / iBlockCounter), 4);
                                meanPrismObs.Add(new Observation()
                                {
                                    Name = strPrismName,
                                    railBracket = strRailBracket,
                                    E = Emean,
                                    N = Nmean,
                                    H = Hmean,
                                    UTCtime = strMeanDate
                                });
                            }

                            iBlockCounter = 0;
                            Emean = 0;
                            Nmean = 0;
                            Hmean = 0;

                            strPreviousMeanDate = strMeanDate;
                            dtTimeBlockStart = dtTimeBlockEnd.AddDays(1);
                            strTimeBlockStart = dtTimeBlockStart.ToString("yyyy-MM-dd 00:00:00");
                            dtTimeBlockStart = DateTime.Parse(strTimeBlockStart, CultureInfo.InvariantCulture);
                            dtTimeBlockEnd = dtTimeBlockStart.AddDays(dblTimeBlockSizeDays);
                            strTimeBlockEnd = dtTimeBlockEnd.ToString("yyyy-MM-dd 23:59:59");
                            strMeanDate = strTimeBlockStart;
                        }


                        iObsCounter++; // Next reading

                    } while (iObsCounter < iNoOfObs);

                    // Flush final block if any remaining
                    if (iBlockCounter > 0)
                    {
                        Emean = Math.Round((Emean / iBlockCounter), 4);
                        Nmean = Math.Round((Nmean / iBlockCounter), 4);
                        Hmean = Math.Round((Hmean / iBlockCounter), 4);
                        meanPrismObs.Add(new Observation()
                        {
                            Name = strPrismName,
                            railBracket = strRailBracket,
                            E = Emean,
                            N = Nmean,
                            H = Hmean,
                            UTCtime = strMeanDate
                        });
                    }

                    for (int ii = 0; ii < meanPrismObs.Count; ii++)
                    {
                        if (Convert.ToString(meanPrismObs[ii].N) == "NaN")
                        {
                            meanPrismObs[ii].E = -99;
                            meanPrismObs[ii].N = -99;
                            meanPrismObs[ii].H = -99;
                            meanPrismObs[ii].dT = -99;
                            meanPrismObs[ii].dH = -99;
                        }
                    }

                    int iNoOfMeanCoordinates = meanPrismObs.Count;

                    if ((iNoOfMeanCoordinates > 0) && (meanPrismObs != null))
                    {
                        for (int iMeanIndex = 0; iMeanIndex < iNoOfMeanCoordinates; iMeanIndex++)
                        {
                            Emean = meanPrismObs[iMeanIndex].E;
                            Nmean = meanPrismObs[iMeanIndex].N;
                            Hmean = meanPrismObs[iMeanIndex].H;

                            double dH = Hmean - Href;
                            var answer = gnaSurvey.Join(Eref, Nref, Emean, Nmean);
                            double dblDisplacementBearing = answer.Item1;
                            double dblDisplacementDist = answer.Item2;

                            double dblYa = Eprev, dblXa = Nprev, dblYb = Eref, dblXb = Nref;
                            double dblYcurrent = Emean, dblXcurrent = Nmean;
                            var answer1 = gnaSurvey.Join(dblYa, dblXa, dblYb, dblXb);
                            double dblRailBearing = answer1.Item1;
                            double dblTransverseBearing = dblRailBearing - (Math.PI / 2);
                            if (dblTransverseBearing < 0) dblTransverseBearing += (2 * Math.PI);

                            var answer3 = gnaSurvey.Join(dblYa, dblXa, dblYb, dblXb);
                            double bearingToSurveyLocation = answer3.Item1;
                            var answer4 = gnaSurvey.Join(dblYa, dblXa, dblYcurrent, dblXcurrent);
                            double bearingToCurrentLocation = answer4.Item1;

                            if (Math.Abs(bearingToSurveyLocation - bearingToCurrentLocation) > Math.PI)
                            {
                                if (bearingToSurveyLocation < bearingToCurrentLocation)
                                    bearingToSurveyLocation += (2 * Math.PI);
                                else
                                    bearingToCurrentLocation += (2 * Math.PI);
                            }

                            var answer2 = gnaSurvey.Intersect(Eref, Nref, dblRailBearing, Emean, Nmean, dblTransverseBearing);
                            double dblTransverseDisplacement = answer2.Item4;

                            if (bearingToCurrentLocation > bearingToSurveyLocation)
                                dblTransverseDisplacement = -dblTransverseDisplacement;

                            if (strStart == "Yes")
                                dblTransverseDisplacement = -dblTransverseDisplacement;

                            meanPrismObs[iMeanIndex].dT = Math.Round(dblTransverseDisplacement, 4);
                            meanPrismObs[iMeanIndex].dH = Math.Round(dH, 4);
                        }
                    }
                    else if (meanPrismObs.Count == 1)
                    {
                        meanPrismObs[0].dT = -99;
                        meanPrismObs[0].dH = -99;
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("getMeanCoordinates failed: ");
                    Console.WriteLine(ex);
                    Console.WriteLine("Press key...");
                    Console.ReadKey();
                }
                finally
                {
                    conn.Close();
                    conn.Dispose();
                }
            }

ExitPoint:
            return meanPrismObs;
        }

        public string getRailBracket(string strDBconnection, string strLongName)
        {

            // Purpose:
            //      To retrieve a railbracket from the database table
            // Input:
            //      as above
            // Output:
            //      Returns the result as a string
            // Useage:
            //      string strResult = gna.getRailBracket(strDBconnection, strRailBracket);
            //

            string strResult = "empty";



            // Connection and Reader declared outside the try block
            using SqlConnection conn = new(strDBconnection);
            conn.Open();
            try
            {

                string SQLaction = @"
                SELECT railBracket 
                FROM fixedData 
                WHERE fixedData.longName = @ELEMENT
                ";

                SqlCommand cmd = new(SQLaction, conn);

                cmd.Parameters.Add(new SqlParameter("@ELEMENT", strLongName));

                // get the value
                strResult = Convert.ToString(cmd.ExecuteScalar());

            }

            catch (SqlException ex)
            {
                Console.WriteLine("getRailBracket failed: ");
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

            catch (SqlException ex)
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


        public Tuple<double, int> extractAverageDistance(string strDBconnection, string strTimeBlockStart, string strTimeBlockEnd, string strSensorName, string strProjectTitle)
        {
            //
            // Purpose:
            //      Extracts the average raw distance from dbo.TMTDistance table for the time block strTimeBlockStart to strTimeBlockEnd.
            // 
            // Input:
            //      Receives 
            //          the target name,
            //          the start and end time blocks.
            // Output:
            //      Returns Tuple<double, int> <average distance, number of distances used>.
            // 
            // Usage:
            //      var answer = gna.extractAverageDistance(strDBconnection, strTimeBlockStart, strTimeBlockEnd, strSensorName, strProjectTitle);
            //      double avgDistance = answer.Item1;
            //      int numElements = answer.Item2;
            //
            // If no data is found, returns:
            //      <0.0, -99>
            //

            double dblAverageDistance = 0.0;
            int intDistanceCounter = 0;

            string strProjectID = getProjectID(strDBconnection, strProjectTitle);
            if (string.IsNullOrEmpty(strProjectID))
            {
                Console.WriteLine("Error: Project Title not found: " + strProjectTitle);
                Console.ReadKey();
                return new Tuple<double, int>(0.0, -99);
            }

            string SQLaction = @"
                SELECT RawDistance
                FROM TMTDistance
                INNER JOIN TMCSensor 
                    ON TMTDistance.SensorID = TMCSensor.ID 
                    AND TMCSensor.Name = @sensorName
                INNER JOIN TMCLocation 
                    ON TMCSensor.LocationID = TMCLocation.ID 
                    AND TMCLocation.ProjectID = @ProjectID
                WHERE TMTDistance.IsOutlier = 0
                AND TMTDistance.EndTimeUTC BETWEEN @startTime AND @endTime";

            using (SqlConnection conn = new SqlConnection(strDBconnection))
            {
                try
                {
                    conn.Open();

                    using (SqlCommand cmd = new SqlCommand(SQLaction, conn))
                    {
                        // Add parameters to prevent SQL injection
                        cmd.Parameters.Add(new SqlParameter("@sensorName", SqlDbType.NVarChar) { Value = strSensorName });
                        cmd.Parameters.Add(new SqlParameter("@ProjectID", SqlDbType.NVarChar) { Value = strProjectID });
                        cmd.Parameters.Add(new SqlParameter("@startTime", SqlDbType.NVarChar) { Value = strTimeBlockStart });
                        cmd.Parameters.Add(new SqlParameter("@endTime", SqlDbType.NVarChar) { Value = strTimeBlockEnd });

                        using (var dataReader = cmd.ExecuteReader())
                        {
                            while (dataReader.Read())
                            {
                                var rawDistanceValue = dataReader["RawDistance"].ToString();
                                if (!string.Equals(rawDistanceValue, "Missing", StringComparison.OrdinalIgnoreCase))
                                {
                                    dblAverageDistance += Convert.ToDouble(rawDistanceValue);
                                    intDistanceCounter++;
                                }
                            }
                        }
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Error:\nextractAverageDistance: DB Connection Failed: " + ex.Message);
                    Console.ReadKey();
                    return new Tuple<double, int>(0.0, -99);
                }
            }

            if (intDistanceCounter > 0)
            {
                dblAverageDistance = Math.Round(dblAverageDistance / intDistanceCounter, 4);
            }
            else
            {
                dblAverageDistance = 0.0;
                intDistanceCounter = -99;
            }

            return new Tuple<double, int>(dblAverageDistance, intDistanceCounter);
        }





        public double getProjectTimeZoneOffset(string strDBconnection, string strProjectTitle)
        {
            //  Local Time = UTC + time offset

            if (string.IsNullOrWhiteSpace(strDBconnection))
                throw new ArgumentException("DB connection string not provided.", nameof(strDBconnection));

            if (string.IsNullOrWhiteSpace(strProjectTitle))
                throw new ArgumentException("Project title not provided.", nameof(strProjectTitle));

            const string sql = @"
                SELECT TOP (1) TimeZoneOffset
                FROM dbo.TMCMonitoringProjects
                WHERE ProjectTitle = @ProjectTitle
                AND IsDeleted = 0;";

            using SqlConnection conn = new(strDBconnection);
            conn.Open();

            using SqlCommand cmd = new(sql, conn);
            cmd.Parameters.Add("@ProjectTitle", SqlDbType.NVarChar, 256).Value = strProjectTitle.Trim();

            object? scalar = cmd.ExecuteScalar();

            if (scalar == null || scalar == DBNull.Value)
                return 0.0;

            return Convert.ToDouble(scalar, CultureInfo.InvariantCulture);
        }

        public string getProjectID(string strDBconnection, string strProjectTitle)
        {
            if (string.IsNullOrWhiteSpace(strDBconnection))
                throw new ArgumentException("DB connection string not provided.", nameof(strDBconnection));

            if (string.IsNullOrWhiteSpace(strProjectTitle))
                throw new ArgumentException("Project title not provided.", nameof(strProjectTitle));

            const string sql = @"
            SELECT TOP (1) ID
            FROM dbo.TMCMonitoringProjects
            WHERE ProjectTitle = @ProjectTitle
            AND IsDeleted = 0;";

            using SqlConnection conn = new(strDBconnection);
            conn.Open();

            using SqlCommand cmd = new(sql, conn);
            cmd.Parameters.Add("@ProjectTitle", SqlDbType.NVarChar, 256).Value = strProjectTitle.Trim();

            object? scalar = cmd.ExecuteScalar();

            if (scalar == null || scalar == DBNull.Value)
                return "Missing";

            return Convert.ToString(scalar, System.Globalization.CultureInfo.InvariantCulture) ?? "Missing";
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



        public string getAlarmStatus(string strDBconnection, string strProjectTitle, string[,] strSensors, double dblAlarmWindowHrs, int iNoOfSuccessfulReadings)
        {
            // Purpose
            //  To return the alarm status of the system - essentially a no data alarm
            // Input
            //  DB connection, DB project title, Sensor name
            // Output
            //  Alarm statu: Alarm/No alarm
            //



            string strLatestObservationUTC = "";
            string strAlarmStatus = "Alarm";
            string strName;
            string strSensorID;
            int iCounter = 0;
            int iSuccessfulReadings = 0;

            string strDateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            string strUTCnow = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

            DateTime dtUTCNow = DateTime.UtcNow;
            CultureInfo provider = CultureInfo.InvariantCulture;

            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                try
                {

                    do
                    {
                        strName = strSensors[iCounter, 0].Trim();
                        strSensorID = strSensors[iCounter, 1].Trim();

                        if (strSensorID != "Missing")
                        {
                            // define the SQL query . Do not try MAX()
                            string SQLaction = @"
                            SELECT TOP(1) TMTPosition_Terrestrial.EndTimeUTC
                            FROM dbo.TMTPosition_Terrestrial 
                            WHERE TMTPosition_Terrestrial.SensorID = @SensorID
                            ORDER BY EndTimeUTC DESC";

                            SqlCommand cmd = new(SQLaction, conn);

                            // define the parameter used in the command object and add to the command
                            cmd.Parameters.Add(new SqlParameter("@SensorID", strSensorID));

                            // Define the data reader
                            SqlDataReader dataReader = cmd.ExecuteReader();

                            // get the values
                            // If the point exists, there will be a result. If not, then the point get marked missing

                            if (!dataReader.Read()) // missing
                            {
                                dataReader.Close();
                                goto NextTarget;
                            }
                            else
                            {
                                strLatestObservationUTC = ((DateTime)dataReader["EndTimeUTC"]).ToString("yyyy-MM-dd HH:mm:ss");
                                DateTime dtLatestObs = DateTime.ParseExact(strLatestObservationUTC.Trim(), "yyyy-MM-dd HH:mm:ss", provider);

                                TimeSpan ts = dtUTCNow.Subtract(dtLatestObs);
                                double dblTimeSpan = ts.TotalHours;

                                //Console.WriteLine(strName + ": " + dtLatestObs + "  "+ dblTimeSpan+ " ("+ dblAlarmWindowHrs+")");

                                if (dblTimeSpan <= dblAlarmWindowHrs)
                                {
                                    iSuccessfulReadings++;
                                }

                                if (iSuccessfulReadings >= iNoOfSuccessfulReadings)
                                {
                                    strAlarmStatus = "OK";
                                    goto ExitHere; // exits in OK state
                                }
                            }
                            dataReader.Close();
                        }
NextTarget:
                        iCounter++;
                        strName = strSensors[iCounter, 0].Trim();
                    } while (strName != "NoMore");

                    strAlarmStatus = strAlarmStatus + "(" + Convert.ToString(iSuccessfulReadings) + "/" + Convert.ToString(iNoOfSuccessfulReadings) + ")"; // Exits in Alarm state

ExitHere:;
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("getAlarmStatus: SQL selection failed: ");
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
            return strAlarmStatus;
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
            // Returns: [PointName, dN, dE, dH, "blank", latestObservationUTC, ObservationCounter]
            // Note: SQL text and query logic preserved exactly.

            // --- Basic input validation ---
            if (strSensorID == null)
                throw new ArgumentNullException(nameof(strSensorID), "Sensor list cannot be null.");

            // Normalize and validate time bounds once (adds :00 if seconds missing).
            strTimeBlockStart = (strTimeBlockStart ?? string.Empty).Trim().Replace("'", "");
            if (strTimeBlockStart.Length == 16) strTimeBlockStart += ":00"; // yyyy-MM-dd HH:mm -> add seconds
            DateTime dtStart;
            if (!DateTime.TryParseExact(strTimeBlockStart, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out dtStart))
                throw new FormatException("strTimeBlockStart must be in format yyyy-MM-dd HH:mm[:ss].");

            strTimeBlockEnd = (strTimeBlockEnd ?? string.Empty).Trim().Replace("'", "");
            if (strTimeBlockEnd.Length == 16) strTimeBlockEnd += ":00";
            DateTime dtEnd;
            if (!DateTime.TryParseExact(strTimeBlockEnd, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out dtEnd))
                throw new FormatException("strTimeBlockEnd must be in format yyyy-MM-dd HH:mm[:ss].");

            // Re-wrap in single quotes for SQL literal usage (keeps your original approach).
            strTimeBlockStart = $"'{dtStart:yyyy-MM-dd HH:mm:ss}'";
            strTimeBlockEnd = $"'{dtEnd:yyyy-MM-dd HH:mm:ss}'";

            string[,] strDeltas = new string[2000, 7];

            int iCounter = 0;
            while (true)
            {
                // Guard against out-of-range indexing (in case "NoMore" sentinel is missing).
                if (iCounter >= strDeltas.GetLength(0))
                    throw new IndexOutOfRangeException("Result buffer exceeded while scanning sensors. Ensure 'NoMore' sentinel is present and size is sufficient.");

                // Defensive fetch of current sensor row
                string strPointName;
                string strPointID;
                try
                {
                    strPointName = strSensorID[iCounter, 0];
                    strPointID = strSensorID[iCounter, 1];
                }
                catch (Exception ex)
                {
                    throw new ArgumentException("Invalid strSensorID array shape or index.", ex);
                }

                if (string.Equals(strPointName, "NoMore", StringComparison.Ordinal))
                    break; // reached sentinel

                double dblLatestdN = 0.0;
                double dblLatestdE = 0.0;
                double dblLatestdH = 0.0;
                int iObservationCounter;
                string strLatestObservationUTC;

                // DB objects
                SqlConnection conn = null;
                SqlCommand cmd = null;
                SqlDataReader dataReader = null;

                try
                {
                    // Instantiate and open connection (keeps your declaration style)
                    conn = new SqlConnection(strDBconnection);
                    conn.Open();

                    if (string.Equals(strPointID, "Missing", StringComparison.Ordinal))
                    {
                        strLatestObservationUTC = "Missing";
                        iObservationCounter = 0;
                        goto PrepareData;
                    }

                    // --- SQL (unchanged) ---
                    string SQLaction = @"
                SELECT * FROM dbo.TMTPosition_Terrestrial  
                WHERE SensorID = @SensorID 
                AND IsOutlier = 0 
                AND EndTimeUTC BETWEEN " + strTimeBlockStart +
                        " AND " + strTimeBlockEnd +
                        " ORDER BY EndTimeUTC DESC";

                    cmd = new SqlCommand(SQLaction, conn);
                    cmd.Parameters.Add(new SqlParameter("@SensorID", strPointID ?? (object)DBNull.Value));

                    dataReader = cmd.ExecuteReader();

                    if (!dataReader.Read())
                    {
                        strLatestObservationUTC = "Missing";
                        iObservationCounter = -99;
                        dblLatestdN = 0;
                        dblLatestdE = 0;
                        dblLatestdH = 0;
                        goto PrepareData;
                    }
                    else
                    {
                        // Safe conversions with DBNull handling; round to 4 dp
                        dblLatestdN = Math.Round(dataReader.IsDBNull(dataReader.GetOrdinal("dN")) ? 0.0 : Convert.ToDouble(dataReader["dN"], CultureInfo.InvariantCulture), 4);
                        dblLatestdE = Math.Round(dataReader.IsDBNull(dataReader.GetOrdinal("dE")) ? 0.0 : Convert.ToDouble(dataReader["dE"], CultureInfo.InvariantCulture), 4);
                        dblLatestdH = Math.Round(dataReader.IsDBNull(dataReader.GetOrdinal("dH")) ? 0.0 : Convert.ToDouble(dataReader["dH"], CultureInfo.InvariantCulture), 4);

                        // Enforce full timestamp with seconds (per your rule)
                        if (!dataReader.IsDBNull(dataReader.GetOrdinal("EndTimeUTC")))
                        {
                            var dt = (DateTime)dataReader["EndTimeUTC"];
                            strLatestObservationUTC = dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            strLatestObservationUTC = "Missing";
                        }

                        iObservationCounter = 1;
                    }
                }
                catch
                {
                    // On any DB/parse error, fall back to "Missing"/-99 and zero deltas
                    strLatestObservationUTC = "Missing";
                    iObservationCounter = -99;
                    dblLatestdN = 0;
                    dblLatestdE = 0;
                    dblLatestdH = 0;
                }
                finally
                {
                    // Dispose in safe order
                    if (dataReader != null)
                    {
                        if (!dataReader.IsClosed) dataReader.Close();
                        dataReader.Dispose();
                    }
                    if (cmd != null) cmd.Dispose();
                    if (conn != null)
                    {
                        if (conn.State != System.Data.ConnectionState.Closed) conn.Close();
                        conn.Dispose();
                    }
                }

PrepareData:
// Insert row
                strDeltas[iCounter, 0] = strPointName;
                strDeltas[iCounter, 1] = Convert.ToString(dblLatestdN, CultureInfo.InvariantCulture);
                strDeltas[iCounter, 2] = Convert.ToString(dblLatestdE, CultureInfo.InvariantCulture);
                strDeltas[iCounter, 3] = Convert.ToString(dblLatestdH, CultureInfo.InvariantCulture);
                strDeltas[iCounter, 4] = "blank";
                strDeltas[iCounter, 5] = strLatestObservationUTC;          // "yyyy-MM-dd HH:mm:ss" or "Missing"
                strDeltas[iCounter, 6] = Convert.ToString(iObservationCounter, CultureInfo.InvariantCulture);

                iCounter++;
            }

            // Sentinel trailer
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



        public Tuple<string, string> getFirstandLastObservationDate(string strDBconnection)
        {
            // Purpose
            //  To return the start and end observation date UTC time for a single point from the Observations table
            // Input
            //  DB connection

            string strLastObservationUTC = "empty";
            string strFirstObservationUTC = "empty";

            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                try
                {
                    // First observation
                    string SQLaction1 = @"
                    SELECT TOP(1) Observations.EndTimeUTC
                    FROM dbo.Observations 
                    ORDER BY EndTimeUTC ASC";

                    SqlCommand cmd = new(SQLaction1, conn);

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the value
                    while (dataReader.Read())
                    {
                        strFirstObservationUTC = ((DateTime)dataReader["EndTimeUTC"]).ToString("yyyy-MM-dd 00:00:00");
                    }

                    // Close the dataReader
                    dataReader?.Close();

                    // Last observation
                    string SQLaction2 = @"
                    SELECT TOP(1) Observations.EndTimeUTC
                    FROM dbo.Observations 
                    ORDER BY EndTimeUTC DESC";

                    cmd = new(SQLaction2, conn);

                    // Define the data reader
                    dataReader = cmd.ExecuteReader();

                    // get the value
                    while (dataReader.Read())
                    {
                        strLastObservationUTC = ((DateTime)dataReader["EndTimeUTC"]).ToString("yyyy-MM-dd 23:59:00");
                    }

                    // Close the dataReader
                    dataReader?.Close();


                }
                catch (SqlException ex)
                {
                    Console.WriteLine("findFirstandLastObservationDate failed: ");
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
            return new Tuple<string, string>(strFirstObservationUTC, strLastObservationUTC);
        }


        public List<Prism> getRailPrismsfromFixedDataTable(string strDBconnection, string strRailBracket)
        {
            // Purpose
            //  to read prism data for a specific rail from fixedData table
            // Output
            //  sorted list of the prisms of a particular rail bracket
            // Use
            //  List<Prism> prism = gnaDBAPI.getRailPrismsfromFixedDataTable(strDBconnection, strRailBracket);


            string strString;
            List<Prism> prism = new();

            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                try
                {
                    // define the SQL query,
                    string SQLaction = @"
                    SELECT * FROM dbo.fixedData
                    WHERE railBracket = @RailBracket
                    ORDER BY longName ASC";

                    SqlCommand cmd = new(SQLaction, conn);
                    // define the parameter used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@RailBracket", strRailBracket));
                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    int iIndex = 0;
                    while (dataReader.Read())
                    {
                        prism.Add(new Prism()
                        {
                            SensorID = Convert.ToString((Int32)dataReader["ID"]),
                            Name = (string)dataReader["longName"],
                            Track = (string)dataReader["railBracket"],
                            dTtrigger = (double)dataReader["dTtrigger"],
                            dHtrigger = (double)dataReader["dHtrigger"],
                            Chainage = (double)dataReader["Chainage"],
                        });
                        iIndex++;
                    }
                    // Close the dataReader
                    dataReader?.Close();

                }
                catch (SqlException ex)
                {
                    Console.WriteLine("getRailPrismsfromFixedDataTable failed: ");
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

            return prism;

        }



        public List<Prism> getPrismsfromFixedDataTable(string strDBconnection)
        {
            // Purpose
            //  to read prism data from fixedData table and write to list
            // Output
            //  All prisams from the fixedData table
            // Use
            //  List<Prism> prism = gnaDBAPI.getPrismsfromFixedDataTable(strDBconnection);

            string strString;
            List<Prism> prism = new();

            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                try
                {
                    // define the SQL query,
                    string SQLaction = @"
                    SELECT * FROM dbo.fixedData
                    ORDER BY ID ASC";

                    SqlCommand cmd = new(SQLaction, conn);

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    int iIndex = 0;
                    while (dataReader.Read())
                    {
                        prism.Add(new Prism()
                        {
                            SensorID = Convert.ToString((Int32)dataReader["ID"]),
                            Name = (string)dataReader["longName"],
                            Track = (string)dataReader["railBracket"],
                            Eref = (double)dataReader["Esurvey"],
                            Nref = (double)dataReader["Nsurvey"],
                            Href = (double)dataReader["Hsurvey"],

                            dTtrigger = (double)dataReader["dTtrigger"],
                            dHtrigger = (double)dataReader["dHtrigger"]
                        });
                        iIndex++;
                    }
                    // Close the dataReader
                    dataReader?.Close();

                }
                catch (SqlException ex)
                {
                    Console.WriteLine("getDatafromFixedDataTable failed: ");
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

            return prism;

        }


        public Tuple<double, double> getMeanDtDh(string strDBconnection, string strTimeBlockStart, string strTimeBlockEnd, int index)
        {
            double dTmean = 0.0;
            double dHmean = 0.0;

            strTimeBlockStart = "'" + strTimeBlockStart + "'";
            strTimeBlockEnd = "'" + strTimeBlockEnd + "'";

            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                try
                {
                    // check whether there are observations
                    string SQLaction0 = @"
                    SELECT COUNT(dH) 
                    FROM Observations
                    WHERE fixedDataPK = @Index
                    AND EndTimeUTC BETWEEN " + strTimeBlockStart + " AND " + strTimeBlockEnd;

                    SqlCommand cmd0 = new(SQLaction0, conn);
                    // define the parameter used in the command object and add to the command
                    cmd0.Parameters.Add(new SqlParameter("@Index", index));

                    if (Convert.ToInt16(cmd0.ExecuteScalar()) == 0)
                    {
                        dTmean = -99.0;
                        dHmean = -99.0;
                        conn.Close();
                        conn.Dispose();
                        goto ExitPoint;
                    }

                    // mean dH
                    string SQLaction1 = @"
                    SELECT AVG(dH) 
                    FROM Observations
                    WHERE fixedDataPK = @Index
                    AND EndTimeUTC BETWEEN " + strTimeBlockStart + " AND " + strTimeBlockEnd;

                    SqlCommand cmd1 = new(SQLaction1, conn);
                    // define the parameter used in the command object and add to the command
                    cmd1.Parameters.Add(new SqlParameter("@Index", index));


                    if (cmd1.ExecuteScalar().ToString != null)
                    {
                        dHmean = Math.Round(Convert.ToDouble(cmd1.ExecuteScalar()), 4);
                    }

                    // mean dT
                    string SQLaction2 = @"
                    SELECT AVG(dT) 
                    FROM Observations
                    WHERE fixedDataPK = @Index
                    AND EndTimeUTC BETWEEN " + strTimeBlockStart + " AND " + strTimeBlockEnd;

                    SqlCommand cmd2 = new(SQLaction2, conn);
                    // define the parameter used in the command object and add to the command
                    cmd2.Parameters.Add(new SqlParameter("@Index", index));

                    if (cmd2.ExecuteScalar() != null)
                    {
                        dTmean = Math.Round(Convert.ToDouble(cmd2.ExecuteScalar()), 4);
                    }

                }
                catch (SqlException ex)
                {
                    Console.WriteLine("findFirstandLastObservationDate failed: ");
                    Console.WriteLine(ex);
                    Console.WriteLine("Press key...");
                    Console.ReadKey();
                }
                finally
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
ExitPoint:
            return new Tuple<double, double>(dTmean, dHmean);

        }


        public List<Observation> getDayDtDh(string strDBconnection, string strWorkbookFullPath, string strCurrentDay, string strRailBracket, List<Prism> prism)
        {
            // Purpose
            //  to read dT dH data from meanObservations table, check that the names match, pad for missing days, and write to list
            // Output
            //  All mean dT and dH readings for that day (1 per prism)
            // Use
            //  List<Observation> dayObs = gnaDBAPI.getDayDtDh(strDBconnection, strWorkbookFullPath, strCurrentDay, strRailBracket, prism);


            string strString;
            string strDate = strCurrentDay;
            strCurrentDay = "'" + strCurrentDay + "'";
            List<Observation> Obs = new();
            List<Observation> dayObs = new();
            using (SqlConnection conn = new(strDBconnection))
            {
                //open connection
                conn.Open();
                try
                {
                    // define the SQL query,
                    string SQLaction = @"
                    SELECT * FROM dbo.meanObservations
                    WHERE EndTImeUTC = " + strCurrentDay +
                    " AND railBracket = @RailBracket ORDER BY Name";

                    SqlCommand cmd = new(SQLaction, conn);
                    // define the parameters used in the command object and add to the command
                    cmd.Parameters.Add(new SqlParameter("@RailBracket", strRailBracket));

                    // Define the data reader
                    SqlDataReader dataReader = cmd.ExecuteReader();

                    // get the values
                    int iIndex = 0;
                    while (dataReader.Read())
                    {
                        Obs.Add(new Observation()
                        {
                            Name = (string)dataReader["Name"],
                            UTCtime = ((DateTime)dataReader["EndTimeUTC"]).ToString("yyyy-MM-dd 00:00:00"),
                            railBracket = (string)dataReader["railBracket"],
                            dT = (double)dataReader["dTmean"],
                            dH = (double)dataReader["dHmean"]
                        });
                        iIndex++;
                    }
                    // Close the dataReader
                    dataReader?.Close();

                    // Address the issue if there are no observations for that day
                    int iNoOfObs = Obs.Count;
                    if ((iNoOfObs == 0) || (iNoOfObs == null))
                    {
                        Obs.Add(new Observation()
                        {
                            Name = prism[0].Name,
                            UTCtime = strCurrentDay,
                            railBracket = strRailBracket,
                            dT = -99,
                            dH = -99
                        });
                    }

                    // Check and pad the dayObs for missing prisms. 
                    iNoOfObs = Obs.Count;
                    int iObsCounter = 0;
                    int iNoOfPrisms = prism.Count;
                    string strPrismName, strObsPrismName;

                    for (int iPrismCounter = 0; iPrismCounter < iNoOfPrisms; iPrismCounter++)
                    {
                        strPrismName = prism[iPrismCounter].Name;
                        strObsPrismName = Obs[iObsCounter].Name;
                        strRailBracket = Obs[iObsCounter].railBracket;
                        double dblDT = Obs[iObsCounter].dT;
                        double dblDH = Obs[iObsCounter].dH;

                        if (strPrismName == strObsPrismName)
                        {
                            dayObs.Add(new Observation()
                            {
                                Name = strPrismName,
                                UTCtime = strCurrentDay,
                                railBracket = strRailBracket,
                                dT = dblDT,
                                dH = dblDH
                            });

                            iObsCounter++;
                            // to accomodate the last prisms being missed so that they can be padded
                            if (iObsCounter >= iNoOfObs)
                            {
                                iObsCounter = (iNoOfObs - 1);
                            }
                            if (iObsCounter < 1)
                            {
                                iObsCounter = 0;
                            }
                        }
                        else
                        {
                            dayObs.Add(new Observation()
                            {
                                Name = strPrismName,
                                UTCtime = strCurrentDay,
                                railBracket = strRailBracket,
                                dT = -99,
                                dH = -99
                            });
                        }
                    }
                }

                catch (SqlException ex)
                {
                    Console.WriteLine("getDayDtDh failed: ");
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

            return dayObs;

        }

        public string[,] getMeanDeltasFromDB(string dbConnection, string projectTitle, string timeBlockStart, string timeBlockEnd, string[,] strSensorID)
        {
            var results = new List<string[]>();
            int iCounter = 0;

            // Outlier Limit
            double dblOutlier = 0.1; // 10 cm threshold for outlier rejection


            while (iCounter < strSensorID.GetLength(0))
            {
                string pointName = strSensorID[iCounter, 0];
                string pointID = strSensorID[iCounter, 1];

                if (pointName == "NoMore")
                    break;

                var deltas = new List<(double dN, double dE, double dH, double dR, double dT)>();
                int obsCount = 0;

                if (pointID != "Missing")
                {
                    using (var conn = new SqlConnection(dbConnection))
                    {
                        conn.Open();

                        string sql = @"
                        SELECT dN, dE, dH, dR, dT 
                        FROM dbo.TMTPosition_Terrestrial
                        WHERE SensorID = @SensorID
                        AND IsOutlier = 0
                        AND EndTimeUTC BETWEEN @StartTime AND @EndTime";

                        using (var cmd = new SqlCommand(sql, conn))
                        {
                            cmd.Parameters.AddWithValue("@SensorID", pointID);
                            cmd.Parameters.AddWithValue("@StartTime", timeBlockStart.Replace("'", ""));
                            cmd.Parameters.AddWithValue("@EndTime", timeBlockEnd.Replace("'", ""));

                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    double dN = Convert.ToDouble(reader["dN"]);
                                    double dE = Convert.ToDouble(reader["dE"]);
                                    double dH = Convert.ToDouble(reader["dH"]);
                                    double dR = Convert.ToDouble(reader["dR"]);
                                    double dT = Convert.ToDouble(reader["dT"]);

                                    if (Math.Abs(dE) < dblOutlier &&
                                        Math.Abs(dN) < dblOutlier &&
                                        Math.Abs(dH) < dblOutlier)
                                    {
                                        deltas.Add((dN, dE, dH, dR, dT));
                                    }

                                }
                            }
                        }
                    }
                }

                double meandN = 0, meandE = 0, meandH = 0, meandR = 0, meandT = 0;
                obsCount = deltas.Count;

                if (pointID != "Missing" && obsCount > 0)
                {

                    if (obsCount > 0)
                    {
                        meandN = Math.Round(deltas.Average(d => d.dN), 4);
                        meandE = Math.Round(deltas.Average(d => d.dE), 4);
                        meandH = Math.Round(deltas.Average(d => d.dH), 4);
                        meandR = Math.Round(deltas.Average(d => d.dR), 4);
                        meandT = Math.Round(deltas.Average(d => d.dT), 4);
                    }
                    else
                    {
                        obsCount = -99;
                    }
                }
                else
                {
                    obsCount = -99;
                }

                results.Add(new string[]
                {
                    pointName,
                    meandN.ToString(),
                    meandE.ToString(),
                    meandH.ToString(),
                    meandR.ToString(),
                    meandT.ToString(),
                    obsCount.ToString()
                });

                iCounter++;
            }

            results.Add(new string[] { "NoMore", "999", "999", "999", "999", "999", "0" });

            int rowCount = results.Count;
            int colCount = 7;
            var strDeltas = new string[rowCount, colCount];

            for (int row = 0; row < rowCount; row++)
            {
                for (int col = 0; col < colCount; col++)
                {
                    strDeltas[row, col] = results[row][col];
                }
            }

            return strDeltas;
        }


        public string[,] getMeanDeltasFromDB_without_filtering(string dbConnection, string projectTitle, string timeBlockStart, string timeBlockEnd, string[,] strSensorID)
        {
            var results = new List<string[]>();
            int iCounter = 0;

            while (iCounter < strSensorID.GetLength(0))
            {
                string pointName = strSensorID[iCounter, 0];
                string pointID = strSensorID[iCounter, 1];

                if (pointName == "NoMore")
                    break;

                double meandN = 0, meandE = 0, meandH = 0, meandR = 0, meandT = 0;
                int obsCount = 0;

                if (pointID != "Missing")
                {
                    using (var conn = new SqlConnection(dbConnection))
                    {
                        conn.Open();

                        string sql = @"
        SELECT 
            AVG(CAST(dN AS FLOAT)) AS MeanN,
            AVG(CAST(dE AS FLOAT)) AS MeanE,
            AVG(CAST(dH AS FLOAT)) AS MeanH,
            AVG(CAST(dR AS FLOAT)) AS MeanR,
            AVG(CAST(dT AS FLOAT)) AS MeanT,
            COUNT(*) AS ObservationCount
        FROM dbo.TMTPosition_Terrestrial
        WHERE SensorID = @SensorID
          AND IsOutlier = 0
          AND EndTimeUTC BETWEEN @StartTime AND @EndTime";

                        using (var cmd = new SqlCommand(sql, conn))
                        {
                            cmd.Parameters.AddWithValue("@SensorID", pointID);
                            cmd.Parameters.AddWithValue("@StartTime", timeBlockStart.Replace("'", ""));
                            cmd.Parameters.AddWithValue("@EndTime", timeBlockEnd.Replace("'", ""));

                            using (var reader = cmd.ExecuteReader())
                            {
                                if (reader.Read() && !reader.IsDBNull(5) && Convert.ToInt32(reader["ObservationCount"]) > 0)
                                {
                                    meandN = Math.Round(Convert.ToDouble(reader["MeanN"]), 4);
                                    meandE = Math.Round(Convert.ToDouble(reader["MeanE"]), 4);
                                    meandH = Math.Round(Convert.ToDouble(reader["MeanH"]), 4);
                                    meandR = Math.Round(Convert.ToDouble(reader["MeanR"]), 4);
                                    meandT = Math.Round(Convert.ToDouble(reader["MeanT"]), 4);
                                    obsCount = Convert.ToInt32(reader["ObservationCount"]);
                                }
                                else
                                {
                                    obsCount = -99;
                                }
                            }
                        }
                    }
                }
                else
                {
                    obsCount = -99;
                }

                results.Add(new string[]
                {
            pointName,
            meandN.ToString(),
            meandE.ToString(),
            meandH.ToString(),
            meandR.ToString(),
            meandT.ToString(),
            obsCount.ToString()
                });

                iCounter++;
            }

            // Add terminator row
            results.Add(new string[] { "NoMore", "999", "999", "999", "999", "999", "0" });

            // Convert to fixed-size 2D array
            int rowCount = results.Count;
            int colCount = 7;
            var strDeltas = new string[rowCount, colCount];

            for (int row = 0; row < rowCount; row++)
            {
                for (int col = 0; col < colCount; col++)
                {
                    strDeltas[row, col] = results[row][col];
                }
            }

            return strDeltas;
        }


        public string[,] getAllDeltasFromDB(string dbConnection, string projectTitle, string timeBlockStart, string timeBlockEnd, string[,] strSensorID)
        {
            string[,] strDeltas = new string[2000, 7];
            int iCounter = 0;

            while (iCounter < strSensorID.GetLength(0))
            {
                string pointName = strSensorID[iCounter, 0];
                string pointID = strSensorID[iCounter, 1];

                if (pointName == "NoMore")
                    break;

                double sumdN = 0, sumdE = 0, sumdH = 0, sumdR = 0, sumdT = 0;
                int obsCount = 0;

                if (pointID != "Missing")
                {
                    using SqlConnection conn = new(dbConnection);
                    conn.Open();

                    string sql = @"
                SELECT dN, dE, dH, dR, dT 
                FROM dbo.TMTPosition_Terrestrial
                WHERE SensorID = @SensorID
                  AND IsOutlier = 0
                  AND EndTimeUTC BETWEEN @StartTime AND @EndTime";

                    using SqlCommand cmd = new(sql, conn);
                    cmd.Parameters.AddWithValue("@SensorID", pointID);
                    cmd.Parameters.AddWithValue("@StartTime", timeBlockStart.Replace("'", ""));
                    cmd.Parameters.AddWithValue("@EndTime", timeBlockEnd.Replace("'", ""));

                    using SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        sumdN += Math.Round(Convert.ToDouble(reader["dN"]), 4);
                        sumdE += Math.Round(Convert.ToDouble(reader["dE"]), 4);
                        sumdH += Math.Round(Convert.ToDouble(reader["dH"]), 4);
                        sumdR += Math.Round(Convert.ToDouble(reader["dR"]), 4);
                        sumdT += Math.Round(Convert.ToDouble(reader["dT"]), 4);
                        obsCount++;
                    }
                }

                // Compute means or assign fallback values
                double meandN = 0, meandE = 0, meandH = 0, meandR = 0, meandT = 0;

                if (pointID == "Missing")
                {
                    obsCount = -99; // flag for no data
                }

                // Assign results
                strDeltas[iCounter, 0] = pointName;
                strDeltas[iCounter, 1] = meandN.ToString();
                strDeltas[iCounter, 2] = meandE.ToString();
                strDeltas[iCounter, 3] = meandH.ToString();
                strDeltas[iCounter, 4] = meandR.ToString();
                strDeltas[iCounter, 5] = meandT.ToString();
                strDeltas[iCounter, 6] = obsCount.ToString();

                iCounter++;
            }

            // Add terminator entry
            strDeltas[iCounter, 0] = "NoMore";
            strDeltas[iCounter, 1] = "999";
            strDeltas[iCounter, 2] = "999";
            strDeltas[iCounter, 3] = "999";
            strDeltas[iCounter, 4] = "999";
            strDeltas[iCounter, 5] = "999";
            strDeltas[iCounter, 6] = "0";

            return strDeltas;
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
                        "INSERT INTO "
                        + strfixedDataTable + " (ID, shortName, longName, railBracket, Esurvey, Nsurvey, Hsurvey, dTtrigger, dHtrigger, Chainage) " + "Values ('"
                        + strID + "','" + fixedData[i].shortName + "','" + fixedData[i].longName + "','" + fixedData[i].railBracket + "','"
                        + fixedData[i].Esurvey.ToString() + "','" + fixedData[i].Nsurvey.ToString() + "','" + fixedData[i].Hsurvey.ToString() + "','"
                        + fixedData[i].dTtrigger.ToString() + "','" + fixedData[i].dHtrigger.ToString() + "','" + fixedData[i].Chainage.ToString() + "')";

                    SqlCommand cmd = new(SQLaction, conn);
                    // define the parameter used in the command object and add to the command
                    cmd.ExecuteNonQuery();
                    i++;
                    strName = fixedData[i].shortName;
                } while (strName != "NoMore");
            }

            catch (SqlException ex)

            {
                Console.WriteLine("putFixedData failed : ");
                Console.WriteLine(ex);
                Console.ReadKey();
            }

            finally
            {
                conn.Dispose();
                conn.Close();
            }
        }

        public void putObservations(string strDBconnection, List<Observation> results, string strObservationsTable)
        {
            // Purpose:
            //      To write the fixed data to the dataAlalysis>fixedData table
            // Input:
            //      array[pointName,replacementName,railBracket, Esurvey,Nsurvey,Hsurvey]
            // Output:
            //      None
            // Useage:
            //     gna.putObservations(strDBconnection, results, strfixedDataTable);
            //

            string strName = "";
            string strUTCtime = "";
            int i = 0;
            int iNoOfResults = results.Count;



            // Connection and Reader declared outside the try block
            using SqlConnection conn = new(strDBconnection);

            //instantiate and open connection#
            conn.Open();
            try
            {
                do
                {
                    strName = results[i].Name;
                    strUTCtime = Convert.ToString(results[i].UTCtime).Trim();
                    string strID = i.ToString();
                    string SQLaction = "INSERT INTO " + strObservationsTable +
" (obsID, fixedDataPK,Name, EndTimeUTC, E,N,H, railBracket) " +
"Values ('" + strID + "','" + results[i].fixedDataIndex + "','" + results[i].Name + "','" + strUTCtime + "','"
+ results[i].E.ToString() + "','" + results[i].N.ToString() + "','" + results[i].H.ToString() + "','" + results[i].railBracket
+ "')";
                    SqlCommand cmd = new(SQLaction, conn);
                    // define the parameter used in the command object and add to the command
                    cmd.ExecuteNonQuery();
                    i++;
                } while (i < iNoOfResults);
            }

            catch (SqlException ex)

            {
                Console.WriteLine("putObservations failed : ");
                Console.WriteLine(ex);
                Console.ReadKey();
            }

            finally
            {
                conn.Dispose();
                conn.Close();
            }


        }


        public void putMeanObsinMeanObservationsTable(string strDBconnection, List<Observation> meanObservations, string strMeanObservationsTable)
        {
            // Purpose:
            //      To write the mean E,N,H, dT, dH to the dataAlalysis>meanObservation table
            // Input:
            //      strDBconnection, results, strfixedDataTable
            // Output:
            //      None
            // Useage:
            //     gnaDBAPI.putMeanObsinMeanObservationsTable(strDBconnection, results, strfixedDataTable);
            //

            int j = 0;
            int maxIndex = meanObservations.Count();

            // Connection and Reader declared outside the try block
            using SqlConnection conn = new(strDBconnection);

            //instantiate and open connection
            conn.Open();
            try
            {
                do
                {
                    string strID = j.ToString();
                    string SQLaction =
                        "INSERT INTO " + strMeanObservationsTable +
                        " (ID, Name, EndTimeUTC,Emean, Nmean, Hmean, dTmean, dHmean, railBracket) " +
                        "Values ('" + strID + "','" + meanObservations[j].Name + "','" + meanObservations[j].UTCtime + "','"
                        + meanObservations[j].E.ToString() + "','" + meanObservations[j].N.ToString() + "','" + meanObservations[j].H.ToString() + "','"
                        + meanObservations[j].dT.ToString() + "','" + meanObservations[j].dH.ToString() + "','" + meanObservations[j].railBracket
                        + "')";

                    SqlCommand cmd = new(SQLaction, conn);
                    // define the parameter used in the command object and add to the command
                    cmd.ExecuteNonQuery();
                    j++;
                } while (j < maxIndex);
            }

            catch (SqlException ex)

            {
                Console.WriteLine("putMeanObsinMeanObservationsTable failed : ");
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

                catch (SqlException ex)
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


        //======[ Unused methods ]=========


        public string[,] getMeanDeltasFromDB_old_version(string dbConnection, string projectTitle, string timeBlockStart, string timeBlockEnd, string[,] strSensorID)
        {
            string[,] strDeltas = new string[2000, 7];
            int iCounter = 0;

            while (iCounter < strSensorID.GetLength(0))
            {
                string pointName = strSensorID[iCounter, 0];
                string pointID = strSensorID[iCounter, 1];

                if (pointName == "NoMore")
                    break;

                double sumdN = 0, sumdE = 0, sumdH = 0, sumdR = 0, sumdT = 0;
                int obsCount = 0;

                if (pointID != "Missing")
                {
                    using SqlConnection conn = new(dbConnection);
                    conn.Open();

                    string sql = @"
                SELECT dN, dE, dH, dR, dT 
                FROM dbo.TMTPosition_Terrestrial
                WHERE SensorID = @SensorID
                  AND IsOutlier = 0
                  AND EndTimeUTC BETWEEN @StartTime AND @EndTime";

                    using SqlCommand cmd = new(sql, conn);
                    cmd.Parameters.AddWithValue("@SensorID", pointID);
                    cmd.Parameters.AddWithValue("@StartTime", timeBlockStart.Replace("'", ""));
                    cmd.Parameters.AddWithValue("@EndTime", timeBlockEnd.Replace("'", ""));

                    using SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        sumdN += Math.Round(Convert.ToDouble(reader["dN"]), 4);
                        sumdE += Math.Round(Convert.ToDouble(reader["dE"]), 4);
                        sumdH += Math.Round(Convert.ToDouble(reader["dH"]), 4);
                        sumdR += Math.Round(Convert.ToDouble(reader["dR"]), 4);
                        sumdT += Math.Round(Convert.ToDouble(reader["dT"]), 4);
                        obsCount++;
                    }
                }

                // Compute means or assign fallback values
                double meandN = 0, meandE = 0, meandH = 0, meandR = 0, meandT = 0;

                if (pointID != "Missing" && obsCount > 0)
                {
                    meandN = Math.Round(sumdN / obsCount, 4);
                    meandE = Math.Round(sumdE / obsCount, 4);
                    meandH = Math.Round(sumdH / obsCount, 4);
                    meandR = Math.Round(sumdR / obsCount, 4);
                    meandT = Math.Round(sumdT / obsCount, 4);
                }
                else
                {
                    obsCount = -99; // flag for no data
                }

                // Assign results
                strDeltas[iCounter, 0] = pointName;
                strDeltas[iCounter, 1] = meandN.ToString();
                strDeltas[iCounter, 2] = meandE.ToString();
                strDeltas[iCounter, 3] = meandH.ToString();
                strDeltas[iCounter, 4] = meandR.ToString();
                strDeltas[iCounter, 5] = meandT.ToString();
                strDeltas[iCounter, 6] = obsCount.ToString();

                iCounter++;
            }

            // Add terminator entry
            strDeltas[iCounter, 0] = "NoMore";
            strDeltas[iCounter, 1] = "999";
            strDeltas[iCounter, 2] = "999";
            strDeltas[iCounter, 3] = "999";
            strDeltas[iCounter, 4] = "999";
            strDeltas[iCounter, 5] = "999";
            strDeltas[iCounter, 6] = "0";

            return strDeltas;
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

        public string[,] getMeanDeltasFromDB_original(string strDBconnection, string strProjectTitle, string strTimeBlockStart, string strTimeBlockEnd, string[,] strSensorID)
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



    }
}
