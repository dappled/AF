package exporter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import utils.ParseDate;
import utils.poi.WriteXls;
import dataWrapper.exporter.portfolioMargin.PMAbstract;
import dataWrapper.exporter.portfolioMargin.PMDailyAnalysis;
import dataWrapper.exporter.portfolioMargin.PMDailyDetail;
import dataWrapper.exporter.portfolioMargin.PMDailyDifference;
import dataWrapper.exporter.portfolioMargin.PMDailyRecord;

/**
 * @author Zhenghong Dong
 */
public class PMExporter extends ExporterBase {
	Map<String, PMDailyAnalysis>	_analysis;

	public PMExporter(String dbServer, String catalog) {
		super( dbServer, catalog );
		_analysis = new LinkedHashMap<>();
	}

	@Override
	public void report(String inFile, String outFile, final String today, final String ftpAddress) throws Exception {
		// generate difference list
		List<PMAbstract> diffList = getDifference( today );

		// generate detail analysis
		List<PMAbstract> detailList = getDetail( today );

		// generate rank list
		List<PMAbstract> rankList = getRank( today );

		exportXls( outFile, diffList, rankList, detailList );
	}

	/** generate daily difference list for given day */
	private List<PMAbstract> getDifference(final String today) {
		List<PMAbstract> diffList = new ArrayList<>();
		final String query = "select * from ( " +
				"select isnull(today.Symbol, yesterday.Symbol) as Sym, today.Requirement as tR, " +
				"yesterday.Requirement as yR, (isnull(today.Requirement,0) - isnull(yesterday.Requirement,0)) as Change from " +
				"((select Symbol, Requirement from Clearing.dbo.PMRequirement " +
				"where importedDate = cast('" + today + "' as Date)) as today " +
				"full outer join " +
				"(select Symbol, Requirement from [Clearing].[dbo].[PMRequirement] " +
				"where importedDate = cast('" + ParseDate.getPreviousWorkingDay( today ) + "' as Date)) as yesterday " +
				"on today.Symbol = yesterday.Symbol)) as tmp " +
				"where tmp.Change <> 0 " +
				"order by Change desc";

		try (Statement stmt = _conn.createStatement()) {

			ResultSet rs = stmt.executeQuery( query );
			while (rs.next()) {
				diffList.add( new PMDailyDifference( today, // importedDate
						rs.getString( 1 ), // symbol
						rs.getDouble( 2 ), // requirementToday
						rs.getDouble( 3 ), // requirementYesterday
						rs.getDouble( 4 ) ) ); // requirementChange
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return diffList;
	}

	/** generate daily rank list according to requirement for given day */
	private List<PMAbstract> getRank(final String today) {
		List<PMAbstract> rankList = new ArrayList<>();
		final String query = "select symbol, symbolType, requirement as Requirement, risk, minimum " +
				"from Clearing.dbo.PMRequirement " +
				"where importedDate = CAST('" + today + "' as DATE) " +
				"order by Requirement desc";

		try (Statement stmt = _conn.createStatement()) {

			ResultSet rs = stmt.executeQuery( query );
			PMDailyRecord record = null;
			PMDailyAnalysis res = null;
			while (rs.next()) {
				record = new PMDailyRecord( today, // importedDate
						rs.getString( 1 ).trim(), // symbol
						rs.getString( 2 ).trim(), // symbolType
						rs.getDouble( 3 ), // requirement
						rs.getDouble( 4 ), // risk
						rs.getDouble( 5 ) ); // minimum
				if ((res = _analysis.get( record.getSymbol() )) != null) {
					record.setReason( res.getReason() );
				}
				rankList.add( record );
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rankList;
	}

	/**
	 * generate daily detailed record report according to PMDetail and PMRequirement
	 * @throws Exception
	 */
	private List<PMAbstract> getDetail(String today) throws Exception {
		// get all PM's detail report whose symbol's requirement = risk on today
		final String query = "SELECT t2.SymbolType,t1.ClassGroupId, t1.productgroupid, t2.Symbol, t1.Symbol, [Maturity],[putCall],[Strike],[Quantity],[Price] "
				+
				",Down5, Down4, Down3, Down2, Down1 "
				+
				",up1, up2, up3, Up4, Up5, Requirement, Risk "
				+
				"FROM "
				+
				"(select * from [Clearing].[dbo].[PMDetail] "
				+
				"where ImportedDate = CAST('"
				+ today
				+ "' as DATE)) as t1 "
				+
				"join "
				+
				"(select Symbol, SymbolType, Requirement, Risk "
				+
				"from Clearing.dbo.PMRequirement "
				+
				"where ImportedDate = CAST('"
				+ today
				+ "' as DATE)) as t2 "
				+
				"on t2.Symbol = t1.ClassGroupId or t2.Symbol = t1.ProductGroupId or (t1.ProductGroupId not in ('00999','00006','00015','00068','00076','00081','00099','00522','00523') and t2.Symbol = 'usidx') "
				+
				"order by Requirement desc";

		try (Statement stmt = _conn.createStatement()) {
			ResultSet rs = stmt.executeQuery( query );
			PMDailyDetail detail = null;
			// this is the actual "symbol", if its product then use productGroupId, if class then use classGroupId, if O then symbol should SPY in detail
			// and USIDX in requirement
			String id = null;

			while (rs.next()) {
				switch (rs.getString( 1 ).trim()) {
					case "C":
						id = rs.getString( 2 ).trim();
						break;
					case "P":
						id = rs.getString( 3 ).trim();
						break;
					case "O":
						id = rs.getString( 4 ).trim(); // use USIDX as id
						break;
					default:
						throw new Exception( "PMExporter: PM record symbol type should be on of C/P/O" );
				}
				int i = 4; // make life easier when add new fields
				detail = new PMDailyDetail( today, // importedDate
						id, // id
						rs.getString( i++ ).trim(), // group symbol
						rs.getString( i++ ).trim(), // ticker symbol
						ParseDate.standardFromSQLDate( rs.getDate( i++ ) ), // maturity
						rs.getString( i++ ).trim(), // putCall
						rs.getDouble( i++ ), // strike
						rs.getDouble( i++ ), // quantity
						rs.getDouble( i++ ), // price
						rs.getDouble( i++ ), // down5
						rs.getDouble( i++ ), // down4
						rs.getDouble( i++ ), // down3
						rs.getDouble( i++ ), // down2
						rs.getDouble( i++ ), // down1
						rs.getDouble( i++ ), // up1
						rs.getDouble( i++ ), // up2
						rs.getDouble( i++ ), // up3
						rs.getDouble( i++ ), // up4
						rs.getDouble( i++ ) ); // up5
				// if this symbol is already in the map, add it the to analysis
				if (!_analysis.containsKey( id )) {
					_analysis.put( id, new PMDailyAnalysis( today, id, rs.getDouble( i++ ), rs.getDouble( i++ ) ) );
				}
				_analysis.get( id ).add( detail );
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		for (PMDailyAnalysis tmpAnalysis : _analysis.values()) {
			tmpAnalysis.analysis();
		}
		return new ArrayList<PMAbstract>( _analysis.values() );
	}

	protected void exportXls(final String outFile, final List<PMAbstract> diffList, final List<PMAbstract> rankList, final List<PMAbstract> detailList)
			throws Exception {
		try {
			final Workbook wb = new HSSFWorkbook();
			// create sheet for difference
			Sheet sheet = wb.createSheet( "difference" );
			// add header for difference
			final CreationHelper createHelper = wb.getCreationHelper();
			Row row = sheet.createRow( (short) 0 );
			row.createCell( 0 ).setCellValue( createHelper.createRichTextString( "Symbol" ) );
			row.createCell( 1 ).setCellValue( createHelper.createRichTextString( "RequirementToday" ) );
			row.createCell( 2 ).setCellValue( createHelper.createRichTextString( "RequirementYesterday" ) );
			row.createCell( 3 ).setCellValue( createHelper.createRichTextString( "Difference" ) );
			// create sheet for rank
			sheet = wb.createSheet( "rank" );
			// add header for rank
			row = sheet.createRow( (short) 0 );
			row.createCell( 0 ).setCellValue( createHelper.createRichTextString( "Symbol" ) );
			row.createCell( 1 ).setCellValue( createHelper.createRichTextString( "Requirement" ) );
			row.createCell( 2 ).setCellValue( createHelper.createRichTextString( "Risk" ) );
			row.createCell( 3 ).setCellValue( createHelper.createRichTextString( "Minimum" ) );
			row.createCell( 4 ).setCellValue( createHelper.createRichTextString( "LargestMove" ) );
			// create sheet for detail
			sheet = wb.createSheet( "details" );
			// save headers
			final FileOutputStream fileOut = new FileOutputStream( outFile );
			wb.write( fileOut );
			fileOut.close();

			// add daily difference
			WriteXls.appendSingleRecord( outFile, "difference", diffList );

			// add daily rank
			WriteXls.appendSingleRecord( outFile, "rank", rankList );

			// add detail reports
			WriteXls.appendSingleRecord( outFile, "details", detailList );

		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}
}
