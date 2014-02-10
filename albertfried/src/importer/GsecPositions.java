package importer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import utils.ParseDate;

/**
 * @author Zhenghong Dong
 */
public class GsecPositions extends ImporterBase {
	public GsecPositions(String dbServer, String catalog) {
		super( dbServer, catalog );
	}

	@Override
	protected void dumpHelper(String localFile, String dbName, String tradeDate) throws Exception {
		PreparedStatement stmt = null;
		String insertString = "insert into " + dbName
				+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		BufferedReader in = null;
		try {
			_conn.setAutoCommit( false );
			stmt = _conn.prepareStatement( insertString );

			in = new BufferedReader( new FileReader( localFile ) );
			String l;
			String[] line;
			in.readLine(); // ignore header
			int count = 0;
			while ((l = in.readLine()) != null) {
				line = l.split( "\\|" );
				int j = 1;
				if (line.length == 5) {
					if (count != Integer.parseInt( line[ 3 ].trim() )) throw new Exception(
							"GsecPositionImporter: " + localFile + " corrupted, number of records read is not same as in the trailer" );
				} else if (line.length != 123) throw new Exception(
						"GsecPositionImporter: " + localFile + " corrupted, inapproporate line: " + line );
				else {
					try {
						// preparation part
						String baseProduct = line[ 14 ].trim();

						String maturity = line[ 59 ].trim() + "/" + line[ 60 ].trim() + "/" + line[ 58 ].trim();
						Date mat = null;
						if (!maturity.equals( "//" )) {
							mat = ParseDate.SQLDateFromStandard( maturity );
						}
						double strike = 0;
						String putCall = null, underlyingSymbol = line[ 16 ].trim();
						if (baseProduct.equals( "OPTION" ) || baseProduct.equals( "FUTFOP" )) {
							strike = Double.parseDouble( line[ 61 ].trim() );
							putCall = line[ 62 ].trim();
							underlyingSymbol = line[ 37 ].trim();
						}
						double bondCouponRate = 0;
						Date bondMat = null;
						if (baseProduct.equals( "BOND" )) {
							bondCouponRate = Double.parseDouble( line[ 67 ].trim() );
							bondMat = ParseDate.SQLDateFromStandard( ParseDate.standardFromyyyyMMdd( line[ 68 ].trim() ) );
						}
						String originalMaturity = line[ 117 ].trim() + "/" + line[ 118 ].trim() + "/" + line[ 116 ].trim();
						Date originalMat = null, nextCouponDate = null, conversionDate = null;
						double pendingPosition = 0;
						if (!originalMaturity.equals( "//" )) {
							originalMat = ParseDate.SQLDateFromStandard( maturity );
						}
						nextCouponDate = ParseDate.SQLDateFromStandard( line[ 119 ].trim() );
						conversionDate = ParseDate.SQLDateFromStandard( ParseDate.standardFromyyyyBMMBdddd( line[ 120 ].trim() ) );
						if (!line[ 121 ].trim().equals( "" )) pendingPosition = Double.parseDouble( line[ 121 ].trim() );

						// write data part
						stmt.setString( j++, line[ 3 ].trim() );// Account
						stmt.setString( j++, line[ 4 ].trim() );// EntityName
						stmt.setString( j++, line[ 6 ].trim() );// ClientAcronym
						stmt.setInt( j++, Integer.parseInt( line[ 7 ].trim() ) );// ClientId
						stmt.setString( j++, baseProduct );// BaseProduct
						stmt.setString( j++, line[ 15 ].trim() );// ProductSubType
						stmt.setString( j++, line[ 16 ].trim() );// TradingSymbol
						stmt.setString( j++, underlyingSymbol );// UnderlyingSymbol
						// TradingProductType1
						String type1 = line[ 17 ].trim();
						if (type1.equals( "" )) stmt.setInt( j++, 0 );
						else stmt.setInt( j++, Integer.parseInt( type1 ) );
						stmt.setString( j++, line[ 18 ].trim() );// TradingProductIdentifier1
						// TradingProductType2
						String type2 = line[ 19 ].trim();
						if (type2.equals( "" )) stmt.setInt( j++, 0 );
						else stmt.setInt( j++, Integer.parseInt( type2 ) );
						stmt.setString( j++, line[ 20 ].trim() );// TradingProductIdentifier2
						// TradingProductType3
						String type3 = line[ 21 ].trim();
						if (type3.equals( "" )) stmt.setInt( j++, 0 );
						else stmt.setInt( j++, Integer.parseInt( type3 ) );
						stmt.setString( j++, line[ 22 ].trim() );// TradingProductIdentifier3

						// maturity, strike, putcall
						if (baseProduct.equals( "OPTION" ) || baseProduct.equals( "FUTFOP" )) {
							stmt.setDate( j++, mat );
							stmt.setDouble( j++, strike );
							stmt.setString( j++, putCall );
						} else {
							stmt.setNull( j++, Types.DATE );
							stmt.setNull( j++, Types.FLOAT );
							stmt.setNull( j++, Types.VARCHAR );
						}

						stmt.setString( j++, line[ 64 ].trim() );// IssueCountry
						stmt.setString( j++, line[ 65 ].trim().replaceAll( "'", ";" ) );// Description
						stmt.setString( j++, line[ 66 ].trim().replaceAll( "'", ";" ) );// Description2

						// bond coupon, mat
						if (baseProduct.equals( "BOND" )) {
							stmt.setDouble( j++, bondCouponRate );
							stmt.setDate( j++, bondMat );
						} else {
							stmt.setNull( j++, Types.FLOAT );
							stmt.setNull( j++, Types.DATE );
						}

						stmt.setDouble( j++, Double.parseDouble( line[ 69 ].trim() ) ); // Multiplier
						stmt.setString( j++, line[ 70 ].trim() ); // FutCommodityCode
						stmt.setString( j++, line[ 71 ].trim() ); // GMIFirm
						stmt.setString( j++, line[ 72 ].trim() ); // GMISalesman
						stmt.setDouble( j++, Double.parseDouble( line[ 73 ].trim() ) ); // TDQuantity
						stmt.setDouble( j++, Double.parseDouble( line[ 74 ].trim() ) );// SDQuantity
						stmt.setDouble( j++, Double.parseDouble( line[ 76 ].trim() ) );// LocalMarketPrice
						stmt.setString( j++, line[ 75 ].trim() );// LocalCurrency
						stmt.setDouble( j++, Double.parseDouble( line[ 77 ].trim() ) );// TDValue
						stmt.setDouble( j++, Double.parseDouble( line[ 78 ].trim() ) );// SDValue
						stmt.setDouble( j++, Double.parseDouble( line[ 81 ].trim() ) );// PrinciplePLDaily
						stmt.setDouble( j++, Double.parseDouble( line[ 82 ].trim() ) );// PrinciplePLMTD
						stmt.setDouble( j++, Double.parseDouble( line[ 83 ].trim() ) );// PrinciplePLYTD
						stmt.setDouble( j++, Double.parseDouble( line[ 84 ].trim() ) );// LocalPLNetDaily
						stmt.setDouble( j++, Double.parseDouble( line[ 85 ].trim() ) );// LocalPLNetMTD
						stmt.setDouble( j++, Double.parseDouble( line[ 86 ].trim() ) );// LocalPLNetYTD
						stmt.setString( j++, line[ 87 ].trim() );// BaseCurrency
						stmt.setDouble( j++, Double.parseDouble( line[ 88 ].trim() ) );// FXRate
						stmt.setString( j++, line[ 89 ].trim() );// FXInd
						stmt.setDouble( j++, Double.parseDouble( line[ 90 ].trim() ) );// BaseMarketPrice
						stmt.setString( j++, line[ 113 ].trim() );// FuturesExchCode
						stmt.setString( j++, line[ 114 ].trim() );// OCCCode
						stmt.setDouble( j++, Double.parseDouble( line[ 115 ].trim() ) );// ConversionRatio

						// original maturity, nextCouponRate,conversionDate
						if (baseProduct.equals( "OPTION" ) || baseProduct.equals( "FUTFOP" )) {
							stmt.setDate( j++, originalMat );
						} else {
							stmt.setNull( j++, Types.DATE );
						}
						if (baseProduct.equals( "BOND" )) {
							stmt.setDate( j++, nextCouponDate );
						} else {
							stmt.setNull( j++, Types.DATE );
						}
						if (conversionDate != null) {
							stmt.setDate( j++, conversionDate );
						} else {
							stmt.setNull( j++, Types.DATE );
						}

						stmt.setDouble( j++, pendingPosition );// PendingPosition
						stmt.setDate( j++, ParseDate.SQLDateFromStandard( tradeDate ) );// ImportedDate
						stmt.executeUpdate();
						_conn.commit();
						count++;
					} catch (SQLException e) {
						e.printStackTrace();
						if (_conn != null) {
							System.err.println( "Transaction is being rolled back" );
							_conn.rollback();
						}
					}
				}
			}
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		} catch (final IOException e) {
			e.printStackTrace();
		} catch (final ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
			if (_conn != null) {
				System.err.print( "Transaction is being rolled back" );
				_conn.rollback();
			}
		} finally {
			if (in != null) in.close();
			if (stmt != null) {
				stmt.close();
			}
			_conn.setAutoCommit( true );
		}
	}
}
