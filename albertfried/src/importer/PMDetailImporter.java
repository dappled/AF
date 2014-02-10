package importer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Zhenghong Dong
 */
public class PMDetailImporter extends ImporterBase {

	public PMDetailImporter(String dbServer, String catalog) {
		super( dbServer, catalog );
	}

	@Override
	protected void dumpHelper(String localFile, String dbName, String tradeDate) throws Exception {
		PreparedStatement insertPMDetail = null;
		String insertString = "insert into " + dbName
				+ " (ImportedDate,ClassGroupId,BasketId,ProductGroupId,Symbol,PutCall,Maturity,Strike,Quantity,Price,Down5,Down4,Down3,Down2,Down1,Up1,Up2,Up3,Up4,Up5)"
				+ " values (cast('" + tradeDate + "' as Date),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		BufferedReader reader = null;
		try {
			// set up statement
			_conn.setAutoCommit( false );
			insertPMDetail = _conn.prepareStatement( insertString );

			// read file
			reader = new BufferedReader( new FileReader( localFile ) );
			reader.readLine(); // ignore header

			// read fields
			String line = null;
			String[] list;
			int count = 0;
			while ((line = reader.readLine()) != null) {
				list = line.split( "\\|" );
				if (list.length == 5) {
					if (count != Integer.parseInt( list[ 3 ].trim() )) throw new Exception(
							"PMDetialImporter: " + localFile + " corrupted, number of records read is not same as in the trailer" );
				} else if (list.length != 36) throw new Exception(
						"PMDetailImporter: " + localFile + " corrupted, inapproporate line: " + line );
				else {
					// insert into database
					insertPMDetail.setString( 1, list[ 14 ].trim() ); // classGroupId
					insertPMDetail.setString( 2, list[ 15 ].trim() ); // basketId
					insertPMDetail.setString( 3, list[ 16 ].trim() ); // productGroupId
					insertPMDetail.setString( 4, list[ 17 ].trim() ); // symbol
					// putCall
					if (list[ 18 ].trim() == "") {
						insertPMDetail.setNull( 5, java.sql.Types.CHAR );
					} else {
						insertPMDetail.setString( 5, list[ 18 ].trim() ); 
					}
					// Maturity
					if (list[19].trim().equals( "" )) {
						insertPMDetail.setNull( 6, java.sql.Types.DATE );
					} else {
						insertPMDetail.setDate( 6, java.sql.Date.valueOf( list[ 19 ].trim()+"-"+list[ 20 ].trim()+"-"+list[ 21 ].trim()) ); // Maturity
					}
					insertPMDetail.setDouble( 7, Double.parseDouble( list[ 22 ].trim() ) ); // strike
					insertPMDetail.setDouble( 8, Double.parseDouble( list[ 23 ].trim() ) ); // quantity
					insertPMDetail.setDouble( 9, Double.parseDouble( list[ 24 ].trim() ) ); // price
					insertPMDetail.setDouble( 10, Double.parseDouble( list[ 25 ].trim() ) ); // down5	
					insertPMDetail.setDouble( 11, Double.parseDouble( list[ 26 ].trim() ) ); // down4
					insertPMDetail.setDouble( 12, Double.parseDouble( list[ 27 ].trim() ) ); // down3
					insertPMDetail.setDouble( 13, Double.parseDouble( list[ 28 ].trim() ) ); // down2
					insertPMDetail.setDouble( 14, Double.parseDouble( list[ 29 ].trim() ) ); // down1
					insertPMDetail.setDouble( 15, Double.parseDouble( list[ 30 ].trim() ) ); // up1
					insertPMDetail.setDouble( 16, Double.parseDouble( list[ 31 ].trim() ) ); // up2
					insertPMDetail.setDouble( 17, Double.parseDouble( list[ 32 ].trim() ) ); // up3
					insertPMDetail.setDouble( 18, Double.parseDouble( list[ 33 ].trim() ) ); // up4
					insertPMDetail.setDouble( 19, Double.parseDouble( list[ 34 ].trim() ) ); // up5
					insertPMDetail.executeUpdate();
					_conn.commit();
					count++;
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
			if (insertPMDetail != null) {
				insertPMDetail.close();
			}
			_conn.setAutoCommit( true );
			reader.close();
		}
	}
}
