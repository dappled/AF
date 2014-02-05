package importer;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * @author Zhenghong Dong
 */
public class ConsolidatedImporter extends ImporterBase {
	/***********************************************************************
	 * Constructor
	 ***********************************************************************/
	public ConsolidatedImporter(String dbServer, String catalog) {
		super( dbServer, catalog );
	}

	@Override
	protected void dumpHelper(String localName, String importName, String tradeDate) throws Exception {
		PreparedStatement importTrade = null;
		String searchString = "select * from " + localName;
		String insertString = "insert into " + importName
				+ " (Symbol,Account,TradeDate,Side,Quantity,Price,UnderlyingSymbol,ExpirationDate,Strike,CallPut,RootCode,Description,Commission) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";

		try {
			// set up statement
			_conn.setAutoCommit( false );
			importTrade = _conn.prepareStatement( insertString );

			// read export db
			try (Statement stmt = _conn.createStatement()) {

				ResultSet rs = stmt.executeQuery( searchString );
				while (rs.next()) {
					int j = 1;
					Date tradeD = rs.getDate( j++ );
					String account = rs.getString( j++ );
					String symbol = rs.getString( j++ );
					Date maturity = rs.getDate( j++ );
					Double strikePrice = rs.getDouble( j++ );
					String pC = rs.getString( j++ );
					String side = rs.getString( j++ );
					Double qty = rs.getDouble( j++ );
					Double avgPrice = rs.getDouble( j++ );
					Double comm = (Double)rs.getObject( j++ );
					String description = rs.getString( j++ );

					j = 1;
					// symbol
					if (symbol == null) importTrade.setNull( j, Types.VARCHAR );
					else importTrade.setString( j, symbol.trim() );
					j++;
					// account
					String a;
					if (account == null) a ="020008832"; 
					account = account.trim();
					switch (account) {
						case "NY81":
						case "WIC01":
							a = "020008832";
							break;
						default:
							a = account;
							break;
					}
					importTrade.setString( j++, a );
					// tradeDate
					if (tradeD == null) importTrade.setNull( j, Types.DATE );
					else importTrade.setDate( j, tradeD);
					j++;
					// side
					if (side == null ) importTrade.setNull( j, Types.VARCHAR );
					else importTrade.setString( j, side.trim() );
					j++;
					// quantity
					importTrade.setDouble( j++,  qty );
					// avgPrice
					importTrade.setDouble( j++, avgPrice );
					// underlying
					if (symbol == null) importTrade.setNull( j, Types.VARCHAR );
					else importTrade.setString( j, symbol.trim() );
					j++;
					// expiration
					if (maturity == null) importTrade.setNull( j, Types.DATE );
					else importTrade.setDate( j, maturity);
					j++;
					// strike
					if (strikePrice == 0) importTrade.setNull( j, Types.FLOAT );
					else importTrade.setDouble( j, strikePrice );
					j++;
					// pC
					if (pC == null ) importTrade.setNull( j, Types.VARCHAR );
					else importTrade.setString( j, pC.trim() );
					j++;
					// rootCode
					if (symbol == null) importTrade.setNull( j, Types.VARCHAR );
					else importTrade.setString( j, symbol.trim() );
					j++;
					// description
					if (description == null ) importTrade.setNull( j, Types.VARCHAR );
					else importTrade.setString( j, description.trim().replaceAll( "'", "" ) );
					j++;
					// commission
					if (comm == null) importTrade.setNull( j, Types.DOUBLE );
					else importTrade.setDouble( j, comm );
					j++;
					
					if (account == null || !account.equals( "NY83" )) {
						importTrade.executeUpdate();
						_conn.commit();
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			if (_conn != null) {
				System.err.print( "Transaction is being rolled back" );
				_conn.rollback();
			}
		} finally {
			if (importTrade != null) {
				importTrade.close();
			}
			_conn.setAutoCommit( true );
		}
	}

}
