using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using VoleraPHDClient;

namespace TestVoleraPhdClient
{
    class Program
    {
        static void runVoleraSnapshotQuery(PhdConnection conn, List<PhdSymbol> phdSymbols, bool ischain, Int64 startDate, Int64 endDate)
        {
            PhdStatement stmt = conn.getStatement();
            PhdParamMap inparams = new PhdParamMap(phdSymbols, startDate, endDate, RequestSnapshot.RequestQueryName);
            inparams.addString(RequestSnapshot.RequestType, RequestSnapshot.REQUEST_TYPE_VOLERA);
            inparams.addInt(RequestSnapshot.RequestChain, (ischain ? 1 : 0));
            inparams.setDateFormat(ColType.DATE_MILLI);
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

		//max of 200 rows returned
        static void runVoleraTicksQuery(PhdConnection conn, List<PhdSymbol> phdSymbols, bool ischain, Int64 startDate, Int64 endDate)
        {
            PhdStatement stmt = conn.getStatement();
            PhdParamMap inparams = new PhdParamMap(phdSymbols, startDate, endDate, RequestTicks.RequestQueryName);
            inparams.addString(RequestTicks.RequestType, RequestTicks.REQUEST_TYPE_VOLERA);
            inparams.addInt(RequestTicks.RequestChain, (ischain ? 1 : 0));
			inparams.setMaxRows(200);
			
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

        //limits result set to 100 rows (new 2012-10)
        static void runTicksQuery(PhdConnection conn, List<PhdSymbol> phdSymbols, bool ischain, Int64 startDate, Int64 endDate){
	        PhdStatement stmt = conn.getStatement();
	        PhdParamMap inparams = new PhdParamMap(phdSymbols, startDate, endDate, RequestTicks.RequestQueryName);
	        inparams.addString(RequestTicks.RequestType, RequestTicks.REQUEST_TYPE_TRADE);
            inparams.addInt(RequestTicks.RequestChain, (ischain ? 1 : 0));

	        List<String> exchanges = new List<String>();
	        exchanges.Add("I"); //ISE trades only
	        inparams.addArrayString(RequestTicks.RequestExchangesOptions, exchanges);

            //inparams.setMaxRows(100);

	        try{
		        PhdResultSet rs = stmt.executeQuery(ref inparams);
                PhdUtils.writeResultSetToCSV(ref rs, Console.Out);
		        rs.close();
		        stmt.close();
	        }catch(PhdException e){
		        Console.WriteLine("RunTicksQuery: Exception - "+e.getMsg());
	        }catch(Exception e){
		        Console.WriteLine("UNKNOWN ERROR - "+e.ToString());
	        }
        }

        static void runReplayQuery(PhdConnection conn, List<PhdSymbol> phdSymbols, bool ischain, Int64 startDate, Int64 endDate, String type, System.IO.TextWriter printstream)
        {
            PhdStatement stmt = conn.getStatement();
            PhdParamMap inparams = new PhdParamMap(phdSymbols, startDate, endDate, RequestReplay.RequestQueryName);
            inparams.addString(RequestReplay.RequestType, type);
            inparams.addInt(RequestReplay.RequestChain, (ischain ? 1 : 0));
            List<String> exchanges = new List<String>();
            exchanges.Add("I"); //ISE trades only
            inparams.addArrayString(RequestTicks.RequestExchangesOptions, exchanges);

            try
            {
                PhdResultSet rs = stmt.executeQuery(ref inparams);
                int count = PhdUtils.writeResultSetToCSV(ref rs, printstream);
                Console.WriteLine("received " + count + " ticks");
                rs.close();
                stmt.close();
            }
            catch (PhdException e)
            {
                Console.WriteLine("RunReplayQuery: Exception - " + e.getMsg());
            }
            catch (Exception e)
            {
                Console.WriteLine("UNKNOWN ERROR - " + e.ToString());
            }
        }


        //10 second BBO mid bars
        static void runOHLCQuery(PhdConnection conn, List<PhdSymbol> phdSymbols, bool ischain, Int64 startDate, Int64 endDate){
	        PhdStatement stmt = conn.getStatement();
	        PhdParamMap inparams = new PhdParamMap(phdSymbols, startDate, endDate, RequestOHLCBars.RequestQueryName);
	        inparams.addString(RequestOHLCBars.RequestType, RequestOHLCBars.REQUEST_TYPE_BBO);
	        inparams.addString(RequestOHLCBars.RequestOHLCType, "MMMM");
            inparams.addInt(RequestOHLCBars.RequestVolume, 1);
	        inparams.addInt(RequestOHLCBars.RequestBucketIntervals, 3600);
            inparams.setApplyTimesDaily(true);
            inparams.addInt(RequestOHLCBars.RequestChain, (ischain ? 1 : 0));
            inparams.addInt(RequestOHLCBars.RequestEquityFactorAdjust, 1);
            inparams.setDateFormat(ColType.DATE_MICRO);
            inparams.setAsOf(endDate);
	        try{
		        PhdResultSet rs = stmt.executeQuery(ref inparams);
                ColInfo[] colInfo = rs.getMetaData();
                for (int i = 0; i < colInfo.Length; i++)
                {
                    Console.Out.WriteLine("Column " + i + ": " + colInfo[i].name + ", " + colInfo[i].dataType.value);
                }
                //PhdUtils.writeResultSetToCSV(ref rs, Console.Out);

                while (rs.next())
                {
                    String symbol = rs.getString(RequestOHLCBars.ReturnSymbol);
                    DateTime dt = PhdUtils.getDateTimeFromMicrosectime(rs.getDate(RequestOHLCBars.ReturnTimestamp));
                    double open = rs.getDouble(RequestOHLCBars.ReturnOpen);
                    double high = rs.getDouble(RequestOHLCBars.ReturnHigh);
                    double low  = rs.getDouble(RequestOHLCBars.ReturnLow);
                    double close = rs.getDouble(RequestOHLCBars.ReturnClose);
                    int volume = rs.getInt(RequestOHLCBars.ReturnVolume);
                    Console.Out.WriteLine(symbol + "," + dt.ToString() + "," + open + "," + high + "," + low + "," + close + "," + volume);
                }

		        rs.close();
		        stmt.close();
	        }catch(PhdException e){
		        Console.WriteLine("RunOHLCQuery: Exception - "+e.getMsg());
	        }catch(Exception e){
		        Console.WriteLine("UNKNOWN ERROR - "+ e.ToString());
	        }
        }

        //chain snapshot for bbo data
        static void runChainSnapshot(PhdConnection conn, List<PhdSymbol> phdSymbols, Int64 startDate, Int64 endDate){
	        PhdStatement stmt = conn.getStatement();
	        PhdParamMap inparams = new PhdParamMap(phdSymbols, startDate, endDate, RequestSnapshot.RequestQueryName);
	        inparams.addInt(RequestSnapshot.RequestChain, 1);
	        inparams.addString(RequestSnapshot.RequestType, RequestSnapshot.REQUEST_TYPE_BBO);
            inparams.addInt(RequestSnapshot.RequestAppendOVSId, 1);

	        try{
		        PhdResultSet rs = stmt.executeQuery(ref inparams);
                PhdUtils.writeResultSetToCSV(ref rs, Console.Out);
		        rs.close();
		        stmt.close();
	        }catch(PhdException e){
		        Console.WriteLine("RunSnapshotQuery: Exception - "+e.getMsg());
	        }catch(Exception e){
		        Console.WriteLine("UNKNOWN ERROR - "+ e.ToString());
	        }
        }

        
        static void runOVSQuery(PhdConnection conn, List<PhdSymbol> phdSymbols, Int64 startDate, Int64 endDate)
        {
            PhdStatement stmt = conn.getStatement();
            PhdParamMap inparams = new PhdParamMap(phdSymbols, startDate, endDate, RequestReference.RequestQueryName);
            inparams.addString(RequestReference.RequestQueryType, RequestReference.REQUEST_TYPE_OPT_CONTRACT);
            inparams.addInt(RequestReference.RequestChain, 1);

            try
            {
                PhdResultSet rs = stmt.executeQuery(ref inparams);
                PhdUtils.writeResultSetToCSV(ref rs, Console.Out);
                rs.close();
                stmt.close();
            }
            catch (PhdException e)
            {
                Console.WriteLine("RunOVSQuery: Exception - " + e.getMsg());
            }
            catch (Exception e)
            {
                Console.WriteLine("UNKNOWN ERROR - " + e.ToString());
            }
        }

        static void runCountQuery(PhdConnection conn, List<PhdSymbol> phdSymbols, bool ischain, Int64 startDate, Int64 endDate)
        {
            PhdStatement stmt = conn.getStatement();
            PhdParamMap inparams = new PhdParamMap(phdSymbols, startDate, endDate, RequestCount.RequestQueryName);
            inparams.addString(RequestCount.RequestType, RequestCount.REQUEST_TYPE_BBO);
            inparams.addInt(RequestCount.RequestChain, (ischain?1:0));
            inparams.addInt(RequestCount.RequestBucketIntervals, 3600);

            try
            {
                PhdResultSet rs = stmt.executeQuery(ref inparams);
                PhdUtils.writeResultSetToCSV(ref rs, Console.Out);
                rs.close();
                stmt.close();
            }
            catch (PhdException e)
            {
                Console.WriteLine("RunCountQuery: Exception - " + e.getMsg());
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
                bool chain = true;

                DateTime startDate = new DateTime(2013, 08, 26, 9, 30, 0);
                DateTime endDate = new DateTime(2013, 08, 26, 16, 00, 0);

                Int64 startMsecTime = PhdUtils.getLongDateTime(startDate);
                Int64 endMsecTime = PhdUtils.getLongDateTime(endDate);
                
                //Console.WriteLine("\nMain: Run Volera Ticks Query");
                //runVoleraTicksQuery(conn, phdSymbols, chain, startMsecTime, endMsecTime);

                //Console.WriteLine("\nMain: Run Level1 Interval Snapshot Query");
                //runIntervalSnapshotQuery(conn, phdSymbols, false, startMsecTime, endMsecTime);

                //Console.WriteLine("\nMain: Run Volera Snapshot Query");
                //runVoleraSnapshotQuery(conn, phdSymbols, true, startMsecTime, endMsecTime);

               // Console.WriteLine("\nMain: Run Ticks Query");
                //runTicksQuery(conn, phdSymbols, chain, startMsecTime, endMsecTime);

                //System.IO.TextWriter fout = new System.IO.StreamWriter("c:\\temp\\csharpout.csv");
                //Console.WriteLine("\nMain: Run Chain TRD Replay Query");
                //runReplayQuery(conn, phdSymbols, true, startMsecTime, endMsecTime, RequestReplay.REQUEST_TYPE_TRADE, Console.Out);
                //fout.Close();

                //Console.WriteLine("\nMain: Run Count Query");
                //runCountQuery(conn, phdSymbols, chain, startMsecTime, endMsecTime);

                //startDate = new DateTime(2012, 12, 01, 9, 30, 0);
                //endDate = new DateTime(2012, 12, 31, 16, 00, 0);
                //startMsecTime = PhdUtils.getLongDateTime(startDate);
                //endMsecTime = PhdUtils.getLongDateTime(endDate);
                //chain = false;
                //Console.WriteLine("\nMain: Run OHLC Query");
                //runOHLCQuery(conn, phdSymbols, chain, startMsecTime, endMsecTime);

                //Console.WriteLine("\nMain: Run Chain Snapshot Query");
                //runChainSnapshot(conn, phdSymbols, startMsecTime, endMsecTime);

                Console.WriteLine("\nMain: Run OVS Query");
                runOVSQuery(conn, phdSymbols, endMsecTime, endMsecTime);
                
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
