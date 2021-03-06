package exporter.validator;

import utils.ParseDate;

public class ValidatorManager {
	private static String			_addressList	= null;
	private static String			_localFile;
	private static String			_brokerFile;
	private static String			_outFile;
	private static ValidatorBase	_validator;
	private static String			_date;
	private static String			_mailSubject;
	private static String			_dbServer;
	private static String			_catalog;
	private static boolean			_wipe			= false;

	public static void main(final String[] args) throws Exception {
		// don't export on holiday, b/c it will be exported next weekday
		if (ParseDate.isHoliday( ParseDate.today) ) return;
		ValidatorManager.parseArgs( args );
		// System.out.println(reader.getDate(gsFile));
		ValidatorManager._validator
				.validate( ValidatorManager._localFile, ValidatorManager._brokerFile,
						ValidatorManager._outFile,
						ValidatorManager._date, _wipe );
		if (_addressList != null) {
			_validator.sendEMail( _outFile, _mailSubject, _addressList, ParseDate.MMddyyyyFromStandard( ParseDate.yesterday ) );
		}
		_validator.close();
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	private static void parseArgs(final String[] args) throws Exception {
		for (int i = 0; i < args.length; i++) {
			switch (args[ i ]) {
				case "/mail":
					ValidatorManager._addressList = args[ ++i ];
					break;
				case "/localFile":
					ValidatorManager._localFile = args[ ++i ];
					if (ValidatorManager._localFile.startsWith( "c:\\" )) {
						ValidatorManager._localFile = ValidatorManager._localFile.replace( "[MMddyyyy]",
								ParseDate.MMddyyyyFromStandard( ParseDate.GSECyesterday ) );
					}
					break;
				case "/brokerFile":
					ValidatorManager._brokerFile = args[ ++i ];
					if (ValidatorManager._brokerFile.startsWith( "c:\\" )) {
						ValidatorManager._brokerFile = ValidatorManager._brokerFile.replace( "[yyyyMMdd]",
								ParseDate.yyyyMMddFromStandard( ParseDate.GSECyesterday ) );
					}
					break;
				case "/outFile":
					ValidatorManager._outFile = args[ ++i ];
					if (ValidatorManager._outFile.startsWith( "c:\\" )) {
						ValidatorManager._outFile = ValidatorManager._outFile.replace( "[MMddyyyy]",
								ParseDate.MMddyyyyFromStandard( ParseDate.yesterday ) );
					}
					break;
				case "/dbserver":
					_dbServer = args[ ++i ];
					break;
				case "/catalog":
					_catalog = args[ ++i ];
					break;
				case "/wipe":
					_wipe = Boolean.parseBoolean( args[ ++i ] );
					break;
				case "/type":
					if (args[ ++i ].equals( "activity" )) {
						_date = ParseDate.GSECyesterday;
						ValidatorManager._validator = new ActivityValidator();
						ValidatorManager._mailSubject = "ActivityMismatchReport";
					}
					else if (args[ i ].equals( "position" )) {
						_date = ParseDate.today;
						ValidatorManager._validator = new PositionValidator( _dbServer, _catalog );
						ValidatorManager._mailSubject = "PostitionMismatchReport";
					} else throw new Exception( "ValidatorManager: /type argument inapproporiate" );
					break;
				default:
					break;
			}
		}
	}
}
