using VoleraPHDClient;

namespace TestVoleraPhdClient
{
    class Program
    {
        static void runIntervalSnapshotQuery(PhdConnection conn, List<PhdSymbol> phdSymbols, bool ischain, Int64 startDate, Int64 endDate)
        {
            PhdStatement stmt = conn.getStatement();
            PhdParamMap inparams = new PhdParamMap(phdSymbols, startDate, endDate, RequestSnapshot.RequestQueryName);
            inparams.addString(RequestSnapshot.RequestType, RequestSnapshot.REQUEST_TYPE_BBO);
            inparams.addInt(RequestSnapshot.RequestChain, (ischain ? 1 : 0));
            inparams.addInt(RequestSnapshot.RequestBucketIntervals, 600);//10 minute bucket

            try
            {
                PhdResultSet rs = stmt.executeQuery(ref inparams);
                PhdUtils.writeResultSetToCSV(ref rs, Console.Out);
                rs.close();
                stmt.close();
            }
            catch (PhdException e)
            {
                Console.WriteLine("RunTicksQuery: Exception - " + e.getMsg());
            }
            catch (Exception e)
            {
                Console.WriteLine("UNKNOWN ERROR - " + e.ToString());
            }
        }

        static void Main(string[] args)
        {
            try
            {
                if (args.Length < 3)
                {
                    Console.WriteLine("Must supply at least 3 arguments: username password ip:port [ip:port (backup)]\n");
                    return;
                }

                PhdConnectionPool connPool = new PhdConnectionPool(args[0], args[1], "phd://"+args[2], (args.Length>3?"phd://"+args[3]:null), false);
                PhdConnection conn = connPool.getConnection();
                List<PhdSymbol> phdSymbols = new List<PhdSymbol>();
                phdSymbols.Add(new PhdSymbol("NKE", PhdSymbol.US_EQUITY));
                //bool chain = true;

                DateTime startDate = new DateTime(2013, 08, 26, 9, 30, 0);
                DateTime endDate = new DateTime(2013, 08, 26, 16, 00, 0);

                Int64 startMsecTime = PhdUtils.getLongDateTime(startDate);
                Int64 endMsecTime = PhdUtils.getLongDateTime(endDate);
                
                Console.WriteLine("\nMain: Run Level1 Interval Snapshot Query");
                runIntervalSnapshotQuery(conn, phdSymbols, false, startMsecTime, endMsecTime);

                connPool.returnConnection(conn);
                connPool.shutDownConnections();
            }
            catch (PhdException e)
            {
                Console.WriteLine(e.getMsg());
                Console.WriteLine(e.StackTrace);
            }
        }
    }
}

