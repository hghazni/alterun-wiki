<?php
// Bail on old versions of PHP, or if composer has not been run yet to install
// dependencies.
require_once __DIR__ . '/../includes/PHPVersionCheck.php';
wfEntryPointCheck( 'cli' );

use MediaWiki\Shell\Shell;
use Wikimedia\Rdbms\DBReplicationWaitError;

// Define this so scripts can easily find doMaintenance.php
define( 'RUN_MAINTENANCE_IF_MAIN', __DIR__ . '/doMaintenance.php' );

define( 'DO_MAINTENANCE', RUN_MAINTENANCE_IF_MAIN ); // original name, harmless

$maintClass = false;

use Wikimedia\Rdbms\IDatabase;
use MediaWiki\Logger\LoggerFactory;
use MediaWiki\MediaWikiServices;
use Wikimedia\Rdbms\LBFactory;
use Wikimedia\Rdbms\IMaintainableDatabase;

abstract class Maintenance {
    const DB_NONE = 0;
    const DB_STD = 1;
    const DB_ADMIN = 2;

    // Const for getStdin()
    const STDIN_ALL = 'all';

    // This is the desired params
    protected $mParams = [];

    // Array of mapping short parameters to long ones
    protected $mShortParamsMap = [];

    // Array of desired args
    protected $mArgList = [];

    // This is the list of options that were actually passed
    protected $mOptions = [];

    // This is the list of arguments that were actually passed
    protected $mArgs = [];

    // Allow arbitrary options to be passed, or only specified ones?
    protected $mAllowUnregisteredOptions = false;

    // Name of the script currently running
    protected $mSelf;

    // Special vars for params that are always used
    protected $mQuiet = false;
    protected $mDbUser, $mDbPass;

    // A description of the script, children should change this via addDescription()
    protected $mDescription = '';

    // Have we already loaded our user input?
    protected $mInputLoaded = false;

    protected $mBatchSize = null;

    // Generic options added by addDefaultParams()
    private $mGenericParameters = [];
    // Generic options which might or not be supported by the script
    private $mDependantParameters = [];

    private $mDb = null;

    private $lastReplicationWait = 0.0;

    public $fileHandle;

    private $config;

    private $requiredExtensions = [];

    public $orderedOptions = [];

    public function __construct() {
        // Setup $IP, using MW_INSTALL_PATH if it exists
        global $IP;
        $IP = strval( getenv( 'MW_INSTALL_PATH' ) ) !== ''
            ? getenv( 'MW_INSTALL_PATH' )
            : realpath( __DIR__ . '/..' );

        $this->addDefaultParams();
        register_shutdown_function( [ $this, 'outputChanneled' ], false );
    }

    public static function shouldExecute() {
        global $wgCommandLineMode;

        if ( !function_exists( 'debug_backtrace' ) ) {
            // If someone has a better idea...
            return $wgCommandLineMode;
        }

        $bt = debug_backtrace();
        $count = count( $bt );
        if ( $count < 2 ) {
            return false; // sanity
        }
        if ( $bt[0]['class'] !== self::class || $bt[0]['function'] !== 'shouldExecute' ) {
            return false; // last call should be to this function
        }
        $includeFuncs = [ 'require_once', 'require', 'include', 'include_once' ];
        for ( $i = 1; $i < $count; $i++ ) {
            if ( !in_array( $bt[$i]['function'], $includeFuncs ) ) {
                return false; // previous calls should all be "requires"
            }
        }

        return true;
    }

    abstract public function execute();

    protected function supportsOption( $name ) {
        return isset( $this->mParams[$name] );
    }

    protected function addOption( $name, $description, $required = false,
                                  $withArg = false, $shortName = false, $multiOccurrence = false
    ) {
        $this->mParams[$name] = [
            'desc' => $description,
            'require' => $required,
            'withArg' => $withArg,
            'shortName' => $shortName,
            'multiOccurrence' => $multiOccurrence
        ];

        if ( $shortName !== false ) {
            $this->mShortParamsMap[$shortName] = $name;
        }
    }

    protected function hasOption( $name ) {
        return isset( $this->mOptions[$name] );
    }

    protected function getOption( $name, $default = null ) {
        if ( $this->hasOption( $name ) ) {
            return $this->mOptions[$name];
        } else {
            // Set it so we don't have to provide the default again
            $this->mOptions[$name] = $default;

            return $this->mOptions[$name];
        }
    }

    protected function addArg( $arg, $description, $required = true ) {
        $this->mArgList[] = [
            'name' => $arg,
            'desc' => $description,
            'require' => $required
        ];
    }

    protected function deleteOption( $name ) {
        unset( $this->mParams[$name] );
    }

    protected function setAllowUnregisteredOptions( $allow ) {
        $this->mAllowUnregisteredOptions = $allow;
    }

    protected function addDescription( $text ) {
        $this->mDescription = $text;
    }

    protected function hasArg( $argId = 0 ) {
        return isset( $this->mArgs[$argId] );
    }

    protected function getArg( $argId = 0, $default = null ) {
        return $this->hasArg( $argId ) ? $this->mArgs[$argId] : $default;
    }

    protected function getBatchSize() {
        return $this->mBatchSize;
    }

    protected function setBatchSize( $s = 0 ) {
        $this->mBatchSize = $s;

        // If we support $mBatchSize, show the option.
        // Used to be in addDefaultParams, but in order for that to
        // work, subclasses would have to call this function in the constructor
        // before they called parent::__construct which is just weird
        // (and really wasn't done).
        if ( $this->mBatchSize ) {
            $this->addOption( 'batch-size', 'Run this many operations ' .
                'per batch, default: ' . $this->mBatchSize, false, true );
            if ( isset( $this->mParams['batch-size'] ) ) {
                // This seems a little ugly...
                $this->mDependantParameters['batch-size'] = $this->mParams['batch-size'];
            }
        }
    }

    public function getName() {
        return $this->mSelf;
    }

    protected function getStdin( $len = null ) {
        if ( $len == self::STDIN_ALL ) {
            return file_get_contents( 'php://stdin' );
        }
        $f = fopen( 'php://stdin', 'rt' );
        if ( !$len ) {
            return $f;
        }
        $input = fgets( $f, $len );
        fclose( $f );

        return rtrim( $input );
    }

    public function isQuiet() {
        return $this->mQuiet;
    }

    protected function output( $out, $channel = null ) {
        // This is sometimes called very early, before Setup.php is included.
        if ( class_exists( MediaWikiServices::class ) ) {
            // Try to periodically flush buffered metrics to avoid OOMs
            $stats = MediaWikiServices::getInstance()->getStatsdDataFactory();
            if ( $stats->getDataCount() > 1000 ) {
                MediaWiki::emitBufferedStatsdData( $stats, $this->getConfig() );
            }
        }

        if ( $this->mQuiet ) {
            return;
        }
        if ( $channel === null ) {
            $this->cleanupChanneled();
            print $out;
        } else {
            $out = preg_replace( '/\n\z/', '', $out );
            $this->outputChanneled( $out, $channel );
        }
    }

    protected function error( $err, $die = 0 ) {
        if ( intval( $die ) !== 0 ) {
            wfDeprecated( __METHOD__ . '( $err, $die )', '1.31' );
            $this->fatalError( $err, intval( $die ) );
        }
        $this->outputChanneled( false );
        if (
            ( PHP_SAPI == 'cli' || PHP_SAPI == 'phpdbg' ) &&
            !defined( 'MW_PHPUNIT_TEST' )
        ) {
            fwrite( STDERR, $err . "\n" );
        } else {
            print $err;
        }
    }

    protected function fatalError( $msg, $exitCode = 1 ) {
        $this->error( $msg );
        exit( $exitCode );
    }

    private $atLineStart = true;
    private $lastChannel = null;

    public function cleanupChanneled() {
        if ( !$this->atLineStart ) {
            print "\n";
            $this->atLineStart = true;
        }
    }

    public function outputChanneled( $msg, $channel = null ) {
        if ( $msg === false ) {
            $this->cleanupChanneled();

            return;
        }

        // End the current line if necessary
        if ( !$this->atLineStart && $channel !== $this->lastChannel ) {
            print "\n";
        }

        print $msg;

        $this->atLineStart = false;
        if ( $channel === null ) {
            // For unchanneled messages, output trailing newline immediately
            print "\n";
            $this->atLineStart = true;
        }
        $this->lastChannel = $channel;
    }

    public function getDbType() {
        return self::DB_STD;
    }

    protected function addDefaultParams() {
        # Generic (non script dependant) options:

        $this->addOption( 'help', 'Display this help message', false, false, 'h' );
        $this->addOption( 'quiet', 'Whether to suppress non-error output', false, false, 'q' );
        $this->addOption( 'conf', 'Location of LocalSettings.php, if not default', false, true );
        $this->addOption( 'wiki', 'For specifying the wiki ID', false, true );
        $this->addOption( 'globals', 'Output globals at the end of processing for debugging' );
        $this->addOption(
            'memory-limit',
            'Set a specific memory limit for the script, '
            . '"max" for no limit or "default" to avoid changing it',
            false,
            true
        );
        $this->addOption( 'server', "The protocol and server name to use in URLs, e.g. " .
            "http://en.wikipedia.org. This is sometimes necessary because " .
            "server name detection may fail in command line scripts.", false, true );
        $this->addOption( 'profiler', 'Profiler output format (usually "text")', false, true );
        // This is named --mwdebug, because --debug would conflict in the phpunit.php CLI script.
        $this->addOption( 'mwdebug', 'Enable built-in MediaWiki development settings', false, true );

        # Save generic options to display them separately in help
        $this->mGenericParameters = $this->mParams;

        # Script dependant options:

        // If we support a DB, show the options
        if ( $this->getDbType() > 0 ) {
            $this->addOption( 'dbuser', 'The DB user to use for this script', false, true );
            $this->addOption( 'dbpass', 'The password to use for this script', false, true );
            $this->addOption( 'dbgroupdefault', 'The default DB group to use.', false, true );
        }

        # Save additional script dependant options to display
        #  them separately in help
        $this->mDependantParameters = array_diff_key( $this->mParams, $this->mGenericParameters );
    }

    public function getConfig() {
        if ( $this->config === null ) {
            $this->config = MediaWikiServices::getInstance()->getMainConfig();
        }

        return $this->config;
    }

    public function setConfig( Config $config ) {
        $this->config = $config;
    }

    protected function requireExtension( $name ) {
        $this->requiredExtensions[] = $name;
    }

    public function checkRequiredExtensions() {
        $registry = ExtensionRegistry::getInstance();
        $missing = [];
        foreach ( $this->requiredExtensions as $name ) {
            if ( !$registry->isLoaded( $name ) ) {
                $missing[] = $name;
            }
        }

        if ( $missing ) {
            $joined = implode( ', ', $missing );
            $msg = "The following extensions are required to be installed "
                . "for this script to run: $joined. Please enable them and then try again.";
            $this->fatalError( $msg );
        }
    }

    public function setAgentAndTriggers() {
        if ( function_exists( 'posix_getpwuid' ) ) {
            $agent = posix_getpwuid( posix_geteuid() )['name'];
        } else {
            $agent = 'sysadmin';
        }
        $agent .= '@' . wfHostname();

        $lbFactory = MediaWikiServices::getInstance()->getDBLoadBalancerFactory();
        // Add a comment for easy SHOW PROCESSLIST interpretation
        $lbFactory->setAgentName(
            mb_strlen( $agent ) > 15 ? mb_substr( $agent, 0, 15 ) . '...' : $agent
        );
        self::setLBFactoryTriggers( $lbFactory, $this->getConfig() );
    }

    public static function setLBFactoryTriggers( LBFactory $LBFactory, Config $config ) {
        $services = MediaWikiServices::getInstance();
        $stats = $services->getStatsdDataFactory();
        // Hook into period lag checks which often happen in long-running scripts
        $lbFactory = $services->getDBLoadBalancerFactory();
        $lbFactory->setWaitForReplicationListener(
            __METHOD__,
            function () use ( $stats, $config ) {
                // Check config in case of JobRunner and unit tests
                if ( $config->get( 'CommandLineMode' ) ) {
                    DeferredUpdates::tryOpportunisticExecute( 'run' );
                }
                // Try to periodically flush buffered metrics to avoid OOMs
                MediaWiki::emitBufferedStatsdData( $stats, $config );
            }
        );
        // Check for other windows to run them. A script may read or do a few writes
        // to the master but mostly be writing to something else, like a file store.
        $lbFactory->getMainLB()->setTransactionListener(
            __METHOD__,
            function ( $trigger ) use ( $stats, $config ) {
                // Check config in case of JobRunner and unit tests
                if ( $config->get( 'CommandLineMode' ) && $trigger === IDatabase::TRIGGER_COMMIT ) {
                    DeferredUpdates::tryOpportunisticExecute( 'run' );
                }
                // Try to periodically flush buffered metrics to avoid OOMs
                MediaWiki::emitBufferedStatsdData( $stats, $config );
            }
        );
    }

    public function runChild( $maintClass, $classFile = null ) {
        // Make sure the class is loaded first
        if ( !class_exists( $maintClass ) ) {
            if ( $classFile ) {
                require_once $classFile;
            }
            if ( !class_exists( $maintClass ) ) {
                $this->error( "Cannot spawn child: $maintClass" );
            }
        }

        $child = new $maintClass();
        $child->loadParamsAndArgs( $this->mSelf, $this->mOptions, $this->mArgs );
        if ( !is_null( $this->mDb ) ) {
            $child->setDB( $this->mDb );
        }

        return $child;
    }

    public function setup() {
        global $IP, $wgCommandLineMode;

        # Abort if called from a web server
        # wfIsCLI() is not available yet
        if ( PHP_SAPI !== 'cli' && PHP_SAPI !== 'phpdbg' ) {
            $this->fatalError( 'This script must be run from the command line' );
        }

        if ( $IP === null ) {
            $this->fatalError( "\$IP not set, aborting!\n" .
                '(Did you forget to call parent::__construct() in your maintenance script?)' );
        }

        # Make sure we can handle script parameters
        if ( !defined( 'HPHP_VERSION' ) && !ini_get( 'register_argc_argv' ) ) {
            $this->fatalError( 'Cannot get command line arguments, register_argc_argv is set to false' );
        }

        // Send PHP warnings and errors to stderr instead of stdout.
        // This aids in diagnosing problems, while keeping messages
        // out of redirected output.
        if ( ini_get( 'display_errors' ) ) {
            ini_set( 'display_errors', 'stderr' );
        }

        $this->loadParamsAndArgs();
        $this->maybeHelp();

        # Set the memory limit
        # Note we need to set it again later in cache LocalSettings changed it
        $this->adjustMemoryLimit();

        # Set max execution time to 0 (no limit). PHP.net says that
        # "When running PHP from the command line the default setting is 0."
        # But sometimes this doesn't seem to be the case.
        ini_set( 'max_execution_time', 0 );

        # Define us as being in MediaWiki
        define( 'MEDIAWIKI', true );

        $wgCommandLineMode = true;

        # Turn off output buffering if it's on
        while ( ob_get_level() > 0 ) {
            ob_end_flush();
        }

        $this->validateParamsAndArgs();
    }

    public function memoryLimit() {
        $limit = $this->getOption( 'memory-limit', 'max' );
        $limit = trim( $limit, "\" '" ); // trim quotes in case someone misunderstood
        return $limit;
    }

    protected function adjustMemoryLimit() {
        $limit = $this->memoryLimit();
        if ( $limit == 'max' ) {
            $limit = -1; // no memory limit
        }
        if ( $limit != 'default' ) {
            ini_set( 'memory_limit', $limit );
        }
    }

    protected function activateProfiler() {
        global $wgProfiler, $wgProfileLimit, $wgTrxProfilerLimits;

        $output = $this->getOption( 'profiler' );
        if ( !$output ) {
            return;
        }

        if ( is_array( $wgProfiler ) && isset( $wgProfiler['class'] ) ) {
            $class = $wgProfiler['class'];
            $profiler = new $class(
                [ 'sampling' => 1, 'output' => [ $output ] ]
                + $wgProfiler
                + [ 'threshold' => $wgProfileLimit ]
            );
            $profiler->setTemplated( true );
            Profiler::replaceStubInstance( $profiler );
        }

        $trxProfiler = Profiler::instance()->getTransactionProfiler();
        $trxProfiler->setLogger( LoggerFactory::getInstance( 'DBPerformance' ) );
        $trxProfiler->setExpectations( $wgTrxProfilerLimits['Maintenance'], __METHOD__ );
    }

    public function clearParamsAndArgs() {
        $this->mOptions = [];
        $this->mArgs = [];
        $this->mInputLoaded = false;
    }

    public function loadWithArgv( $argv ) {
        $options = [];
        $args = [];
        $this->orderedOptions = [];

        # Parse arguments
        for ( $arg = reset( $argv ); $arg !== false; $arg = next( $argv ) ) {
            if ( $arg == '--' ) {
                # End of options, remainder should be considered arguments
                $arg = next( $argv );
                while ( $arg !== false ) {
                    $args[] = $arg;
                    $arg = next( $argv );
                }
                break;
            } elseif ( substr( $arg, 0, 2 ) == '--' ) {
                # Long options
                $option = substr( $arg, 2 );
                if ( isset( $this->mParams[$option] ) && $this->mParams[$option]['withArg'] ) {
                    $param = next( $argv );
                    if ( $param === false ) {
                        $this->error( "\nERROR: $option parameter needs a value after it\n" );
                        $this->maybeHelp( true );
                    }

                    $this->setParam( $options, $option, $param );
                } else {
                    $bits = explode( '=', $option, 2 );
                    if ( count( $bits ) > 1 ) {
                        $option = $bits[0];
                        $param = $bits[1];
                    } else {
                        $param = 1;
                    }

                    $this->setParam( $options, $option, $param );
                }
            } elseif ( $arg == '-' ) {
                # Lonely "-", often used to indicate stdin or stdout.
                $args[] = $arg;
            } elseif ( substr( $arg, 0, 1 ) == '-' ) {
                # Short options
                $argLength = strlen( $arg );
                for ( $p = 1; $p < $argLength; $p++ ) {
                    $option = $arg[$p];
                    if ( !isset( $this->mParams[$option] ) && isset( $this->mShortParamsMap[$option] ) ) {
                        $option = $this->mShortParamsMap[$option];
                    }

                    if ( isset( $this->mParams[$option]['withArg'] ) && $this->mParams[$option]['withArg'] ) {
                        $param = next( $argv );
                        if ( $param === false ) {
                            $this->error( "\nERROR: $option parameter needs a value after it\n" );
                            $this->maybeHelp( true );
                        }
                        $this->setParam( $options, $option, $param );
                    } else {
                        $this->setParam( $options, $option, 1 );
                    }
                }
            } else {
                $args[] = $arg;
            }
        }

        $this->mOptions = $options;
        $this->mArgs = $args;
        $this->loadSpecialVars();
        $this->mInputLoaded = true;
    }

    private function setParam( &$options, $option, $value ) {
        $this->orderedOptions[] = [ $option, $value ];

        if ( isset( $this->mParams[$option] ) ) {
            $multi = $this->mParams[$option]['multiOccurrence'];
        } else {
            $multi = false;
        }
        $exists = array_key_exists( $option, $options );
        if ( $multi && $exists ) {
            $options[$option][] = $value;
        } elseif ( $multi ) {
            $options[$option] = [ $value ];
        } elseif ( !$exists ) {
            $options[$option] = $value;
        } else {
            $this->error( "\nERROR: $option parameter given twice\n" );
            $this->maybeHelp( true );
        }
    }

    public function loadParamsAndArgs( $self = null, $opts = null, $args = null ) {
        # If we were given opts or args, set those and return early
        if ( $self ) {
            $this->mSelf = $self;
            $this->mInputLoaded = true;
        }
        if ( $opts ) {
            $this->mOptions = $opts;
            $this->mInputLoaded = true;
        }
        if ( $args ) {
            $this->mArgs = $args;
            $this->mInputLoaded = true;
        }

        # If we've already loaded input (either by user values or from $argv)
        # skip on loading it again. The array_shift() will corrupt values if
        # it's run again and again
        if ( $this->mInputLoaded ) {
            $this->loadSpecialVars();

            return;
        }

        global $argv;
        $this->mSelf = $argv[0];
        $this->loadWithArgv( array_slice( $argv, 1 ) );
    }

    protected function validateParamsAndArgs() {
        $die = false;
        # Check to make sure we've got all the required options
        foreach ( $this->mParams as $opt => $info ) {
            if ( $info['require'] && !$this->hasOption( $opt ) ) {
                $this->error( "Param $opt required!" );
                $die = true;
            }
        }
        # Check arg list too
        foreach ( $this->mArgList as $k => $info ) {
            if ( $info['require'] && !$this->hasArg( $k ) ) {
                $this->error( 'Argument <' . $info['name'] . '> required!' );
                $die = true;
            }
        }
        if ( !$this->mAllowUnregisteredOptions ) {
            # Check for unexpected options
            foreach ( $this->mOptions as $opt => $val ) {
                if ( !$this->supportsOption( $opt ) ) {
                    $this->error( "Unexpected option $opt!" );
                    $die = true;
                }
            }
        }

        if ( $die ) {
            $this->maybeHelp( true );
        }
    }

    protected function loadSpecialVars() {
        if ( $this->hasOption( 'dbuser' ) ) {
            $this->mDbUser = $this->getOption( 'dbuser' );
        }
        if ( $this->hasOption( 'dbpass' ) ) {
            $this->mDbPass = $this->getOption( 'dbpass' );
        }
        if ( $this->hasOption( 'quiet' ) ) {
            $this->mQuiet = true;
        }
        if ( $this->hasOption( 'batch-size' ) ) {
            $this->mBatchSize = intval( $this->getOption( 'batch-size' ) );
        }
    }

    protected function maybeHelp( $force = false ) {
        if ( !$force && !$this->hasOption( 'help' ) ) {
            return;
        }

        $screenWidth = 80; // TODO: Calculate this!
        $tab = "    ";
        $descWidth = $screenWidth - ( 2 * strlen( $tab ) );

        ksort( $this->mParams );
        $this->mQuiet = false;

        // Description ...
        if ( $this->mDescription ) {
            $this->output( "\n" . wordwrap( $this->mDescription, $screenWidth ) . "\n" );
        }
        $output = "\nUsage: php " . basename( $this->mSelf );

        // ... append parameters ...
        if ( $this->mParams ) {
            $output .= " [--" . implode( "|--", array_keys( $this->mParams ) ) . "]";
        }

        // ... and append arguments.
        if ( $this->mArgList ) {
            $output .= ' ';
            foreach ( $this->mArgList as $k => $arg ) {
                if ( $arg['require'] ) {
                    $output .= '<' . $arg['name'] . '>';
                } else {
                    $output .= '[' . $arg['name'] . ']';
                }
                if ( $k < count( $this->mArgList ) - 1 ) {
                    $output .= ' ';
                }
            }
        }
        $this->output( "$output\n\n" );

        # TODO abstract some repetitive code below

        // Generic parameters
        $this->output( "Generic maintenance parameters:\n" );
        foreach ( $this->mGenericParameters as $par => $info ) {
            if ( $info['shortName'] !== false ) {
                $par .= " (-{$info['shortName']})";
            }
            $this->output(
                wordwrap( "$tab--$par: " . $info['desc'], $descWidth,
                    "\n$tab$tab" ) . "\n"
            );
        }
        $this->output( "\n" );

        $scriptDependantParams = $this->mDependantParameters;
        if ( count( $scriptDependantParams ) > 0 ) {
            $this->output( "Script dependant parameters:\n" );
            // Parameters description
            foreach ( $scriptDependantParams as $par => $info ) {
                if ( $info['shortName'] !== false ) {
                    $par .= " (-{$info['shortName']})";
                }
                $this->output(
                    wordwrap( "$tab--$par: " . $info['desc'], $descWidth,
                        "\n$tab$tab" ) . "\n"
                );
            }
            $this->output( "\n" );
        }

        // Script specific parameters not defined on construction by
        // Maintenance::addDefaultParams()
        $scriptSpecificParams = array_diff_key(
        # all script parameters:
            $this->mParams,
            # remove the Maintenance default parameters:
            $this->mGenericParameters,
            $this->mDependantParameters
        );
        if ( count( $scriptSpecificParams ) > 0 ) {
            $this->output( "Script specific parameters:\n" );
            // Parameters description
            foreach ( $scriptSpecificParams as $par => $info ) {
                if ( $info['shortName'] !== false ) {
                    $par .= " (-{$info['shortName']})";
                }
                $this->output(
                    wordwrap( "$tab--$par: " . $info['desc'], $descWidth,
                        "\n$tab$tab" ) . "\n"
                );
            }
            $this->output( "\n" );
        }

        // Print arguments
        if ( count( $this->mArgList ) > 0 ) {
            $this->output( "Arguments:\n" );
            // Arguments description
            foreach ( $this->mArgList as $info ) {
                $openChar = $info['require'] ? '<' : '[';
                $closeChar = $info['require'] ? '>' : ']';
                $this->output(
                    wordwrap( "$tab$openChar" . $info['name'] . "$closeChar: " .
                        $info['desc'], $descWidth, "\n$tab$tab" ) . "\n"
                );
            }
            $this->output( "\n" );
        }

        die( 1 );
    }

    public function finalSetup() {
        global $wgCommandLineMode, $wgServer, $wgShowExceptionDetails, $wgShowHostnames;
        global $wgDBadminuser, $wgDBadminpassword, $wgDBDefaultGroup;
        global $wgDBuser, $wgDBpassword, $wgDBservers, $wgLBFactoryConf;

        # Turn off output buffering again, it might have been turned on in the settings files
        if ( ob_get_level() ) {
            ob_end_flush();
        }
        # Same with these
        $wgCommandLineMode = true;

        # Override $wgServer
        if ( $this->hasOption( 'server' ) ) {
            $wgServer = $this->getOption( 'server', $wgServer );
        }

        # If these were passed, use them
        if ( $this->mDbUser ) {
            $wgDBadminuser = $this->mDbUser;
        }
        if ( $this->mDbPass ) {
            $wgDBadminpassword = $this->mDbPass;
        }
        if ( $this->hasOption( 'dbgroupdefault' ) ) {
            $wgDBDefaultGroup = $this->getOption( 'dbgroupdefault', null );

            MediaWikiServices::getInstance()->getDBLoadBalancerFactory()->destroy();
        }

        if ( $this->getDbType() == self::DB_ADMIN && isset( $wgDBadminuser ) ) {
            $wgDBuser = $wgDBadminuser;
            $wgDBpassword = $wgDBadminpassword;

            if ( $wgDBservers ) {
                foreach ( $wgDBservers as $i => $server ) {
                    $wgDBservers[$i]['user'] = $wgDBuser;
                    $wgDBservers[$i]['password'] = $wgDBpassword;
                }
            }
            if ( isset( $wgLBFactoryConf['serverTemplate'] ) ) {
                $wgLBFactoryConf['serverTemplate']['user'] = $wgDBuser;
                $wgLBFactoryConf['serverTemplate']['password'] = $wgDBpassword;
            }
            MediaWikiServices::getInstance()->getDBLoadBalancerFactory()->destroy();
        }

        # Apply debug settings
        if ( $this->hasOption( 'mwdebug' ) ) {
            require __DIR__ . '/../includes/DevelopmentSettings.php';
        }

        // Per-script profiling; useful for debugging
        $this->activateProfiler();

        $this->afterFinalSetup();

        $wgShowExceptionDetails = true;
        $wgShowHostnames = true;

        Wikimedia\suppressWarnings();
        set_time_limit( 0 );
        Wikimedia\restoreWarnings();

        $this->adjustMemoryLimit();
    }

    protected function afterFinalSetup() {
        if ( defined( 'MW_CMDLINE_CALLBACK' ) ) {
            call_user_func( MW_CMDLINE_CALLBACK );
        }
    }

    public function globals() {
        if ( $this->hasOption( 'globals' ) ) {
            print_r( $GLOBALS );
        }
    }

    public function loadSettings() {
        global $wgCommandLineMode, $IP;

        if ( isset( $this->mOptions['conf'] ) ) {
            $settingsFile = $this->mOptions['conf'];
        } elseif ( defined( "MW_CONFIG_FILE" ) ) {
            $settingsFile = MW_CONFIG_FILE;
        } else {
            $settingsFile = "$IP/LocalSettings.php";
        }
        if ( isset( $this->mOptions['wiki'] ) ) {
            $bits = explode( '-', $this->mOptions['wiki'] );
            if ( count( $bits ) == 1 ) {
                $bits[] = '';
            }
            define( 'MW_DB', $bits[0] );
            define( 'MW_PREFIX', $bits[1] );
        } elseif ( isset( $this->mOptions['server'] ) ) {
            // Provide the option for site admins to detect and configure
            // multiple wikis based on server names. This offers --server
            // as alternative to --wiki.
            // See https://www.mediawiki.org/wiki/Manual:Wiki_family
            $_SERVER['SERVER_NAME'] = $this->mOptions['server'];
        }

        if ( !is_readable( $settingsFile ) ) {
            $this->fatalError( "A copy of your installation's LocalSettings.php\n" .
                "must exist and be readable in the source directory.\n" .
                "Use --conf to specify it." );
        }
        $wgCommandLineMode = true;

        return $settingsFile;
    }

    public function purgeRedundantText( $delete = true ) {
        # Data should come off the master, wrapped in a transaction
        $dbw = $this->getDB( DB_MASTER );
        $this->beginTransaction( $dbw, __METHOD__ );

        # Get "active" text records from the revisions table
        $cur = [];
        $this->output( 'Searching for active text records in revisions table...' );
        $res = $dbw->select( 'revision', 'rev_text_id', [], __METHOD__, [ 'DISTINCT' ] );
        foreach ( $res as $row ) {
            $cur[] = $row->rev_text_id;
        }
        $this->output( "done.\n" );

        # Get "active" text records from the archive table
        $this->output( 'Searching for active text records in archive table...' );
        $res = $dbw->select( 'archive', 'ar_text_id', [], __METHOD__, [ 'DISTINCT' ] );
        foreach ( $res as $row ) {
            # old pre-MW 1.5 records can have null ar_text_id's.
            if ( $row->ar_text_id !== null ) {
                $cur[] = $row->ar_text_id;
            }
        }
        $this->output( "done.\n" );

        # Get the IDs of all text records not in these sets
        $this->output( 'Searching for inactive text records...' );
        $cond = 'old_id NOT IN ( ' . $dbw->makeList( $cur ) . ' )';
        $res = $dbw->select( 'text', 'old_id', [ $cond ], __METHOD__, [ 'DISTINCT' ] );
        $old = [];
        foreach ( $res as $row ) {
            $old[] = $row->old_id;
        }
        $this->output( "done.\n" );

        # Inform the user of what we're going to do
        $count = count( $old );
        $this->output( "$count inactive items found.\n" );

        # Delete as appropriate
        if ( $delete && $count ) {
            $this->output( 'Deleting...' );
            $dbw->delete( 'text', [ 'old_id' => $old ], __METHOD__ );
            $this->output( "done.\n" );
        }

        # Done
        $this->commitTransaction( $dbw, __METHOD__ );
    }

    protected function getDir() {
        return __DIR__;
    }

    protected function getDB( $db, $groups = [], $wiki = false ) {
        if ( is_null( $this->mDb ) ) {
            return wfGetDB( $db, $groups, $wiki );
        } else {
            return $this->mDb;
        }
    }

    public function setDB( IDatabase $db ) {
        $this->mDb = $db;
    }

    protected function beginTransaction( IDatabase $dbw, $fname ) {
        $dbw->begin( $fname );
    }

    protected function commitTransaction( IDatabase $dbw, $fname ) {
        $dbw->commit( $fname );
        try {
            $lbFactory = MediaWikiServices::getInstance()->getDBLoadBalancerFactory();
            $lbFactory->waitForReplication(
                [ 'timeout' => 30, 'ifWritesSince' => $this->lastReplicationWait ]
            );
            $this->lastReplicationWait = microtime( true );

            return true;
        } catch ( DBReplicationWaitError $e ) {
            return false;
        }
    }

    protected function rollbackTransaction( IDatabase $dbw, $fname ) {
        $dbw->rollback( $fname );
    }

    private function lockSearchindex( $db ) {
        $write = [ 'searchindex' ];
        $read = [
            'page',
            'revision',
            'text',
            'interwiki',
            'l10n_cache',
            'user',
            'page_restrictions'
        ];
        $db->lockTables( $read, $write, __CLASS__ . '::' . __METHOD__ );
    }

    private function unlockSearchindex( $db ) {
        $db->unlockTables( __CLASS__ . '::' . __METHOD__ );
    }

    private function relockSearchindex( $db ) {
        $this->unlockSearchindex( $db );
        $this->lockSearchindex( $db );
    }

    public function updateSearchIndex( $maxLockTime, $callback, $dbw, $results ) {
        $lockTime = time();

        # Lock searchindex
        if ( $maxLockTime ) {
            $this->output( "   --- Waiting for lock ---" );
            $this->lockSearchindex( $dbw );
            $lockTime = time();
            $this->output( "\n" );
        }

        # Loop through the results and do a search update
        foreach ( $results as $row ) {
            # Allow reads to be processed
            if ( $maxLockTime && time() > $lockTime + $maxLockTime ) {
                $this->output( "    --- Relocking ---" );
                $this->relockSearchindex( $dbw );
                $lockTime = time();
                $this->output( "\n" );
            }
            call_user_func( $callback, $dbw, $row );
        }

        # Unlock searchindex
        if ( $maxLockTime ) {
            $this->output( "    --- Unlocking --" );
            $this->unlockSearchindex( $dbw );
            $this->output( "\n" );
        }
    }

    public function updateSearchIndexForPage( $dbw, $pageId ) {
        // Get current revision
        $rev = Revision::loadFromPageId( $dbw, $pageId );
        $title = null;
        if ( $rev ) {
            $titleObj = $rev->getTitle();
            $title = $titleObj->getPrefixedDBkey();
            $this->output( "$title..." );
            # Update searchindex
            $u = new SearchUpdate( $pageId, $titleObj->getText(), $rev->getContent() );
            $u->doUpdate();
            $this->output( "\n" );
        }

        return $title;
    }

    protected function countDown( $seconds ) {
        if ( $this->isQuiet() ) {
            return;
        }
        for ( $i = $seconds; $i >= 0; $i-- ) {
            if ( $i != $seconds ) {
                $this->output( str_repeat( "\x08", strlen( $i + 1 ) ) );
            }
            $this->output( $i );
            if ( $i ) {
                sleep( 1 );
            }
        }
        $this->output( "\n" );
    }

    public static function posix_isatty( $fd ) {
        if ( !function_exists( 'posix_isatty' ) ) {
            return !$fd;
        } else {
            return posix_isatty( $fd );
        }
    }

    public static function readconsole( $prompt = '> ' ) {
        static $isatty = null;
        if ( is_null( $isatty ) ) {
            $isatty = self::posix_isatty( 0 /*STDIN*/ );
        }

        if ( $isatty && function_exists( 'readline' ) ) {
            return readline( $prompt );
        } else {
            if ( $isatty ) {
                $st = self::readlineEmulation( $prompt );
            } else {
                if ( feof( STDIN ) ) {
                    $st = false;
                } else {
                    $st = fgets( STDIN, 1024 );
                }
            }
            if ( $st === false ) {
                return false;
            }
            $resp = trim( $st );

            return $resp;
        }
    }

    private static function readlineEmulation( $prompt ) {
        $bash = ExecutableFinder::findInDefaultPaths( 'bash' );
        if ( !wfIsWindows() && $bash ) {
            $retval = false;
            $encPrompt = wfEscapeShellArg( $prompt );
            $command = "read -er -p $encPrompt && echo \"\$REPLY\"";
            $encCommand = wfEscapeShellArg( $command );
            $line = wfShellExec( "$bash -c $encCommand", $retval, [], [ 'walltime' => 0 ] );

            if ( $retval == 0 ) {
                return $line;
            } elseif ( $retval == 127 ) {
                // Couldn't execute bash even though we thought we saw it.
                // Shell probably spit out an error message, sorry :(
                // Fall through to fgets()...
            } else {
                // EOF/ctrl+D
                return false;
            }
        }

        // Fallback... we'll have no editing controls, EWWW
        if ( feof( STDIN ) ) {
            return false;
        }
        print $prompt;

        return fgets( STDIN, 1024 );
    }

    public static function getTermSize() {
        $default = [ 80, 50 ];
        if ( wfIsWindows() ) {
            return $default;
        }
        if ( Shell::isDisabled() ) {
            return $default;
        }
        // It's possible to get the screen size with VT-100 terminal escapes,
        // but reading the responses is not possible without setting raw mode
        // (unless you want to require the user to press enter), and that
        // requires an ioctl(), which we can't do. So we have to shell out to
        // something that can do the relevant syscalls. There are a few
        // options. Linux and Mac OS X both have "stty size" which does the
        // job directly.
        $result = Shell::command( 'stty', 'size' )
            ->execute();
        if ( $result->getExitCode() !== 0 ) {
            return $default;
        }
        if ( !preg_match( '/^(\d+) (\d+)$/', $result->getStdout(), $m ) ) {
            return $default;
        }
        return [ intval( $m[2] ), intval( $m[1] ) ];
    }

    public static function requireTestsAutoloader() {
        require_once __DIR__ . '/../tests/common/TestsAutoLoader.php';
    }
}

class FakeMaintenance extends Maintenance {
    protected $mSelf = "FakeMaintenanceScript";

    public function execute() {
        return;
    }
}

abstract class LoggedUpdateMaintenance extends Maintenance {
    public function __construct() {
        parent::__construct();
        $this->addOption( 'force', 'Run the update even if it was completed already' );
        $this->setBatchSize( 200 );
    }

    public function execute() {
        $db = $this->getDB( DB_MASTER );
        $key = $this->getUpdateKey();

        if ( !$this->hasOption( 'force' )
            && $db->selectRow( 'updatelog', '1', [ 'ul_key' => $key ], __METHOD__ )
        ) {
            $this->output( "..." . $this->updateSkippedMessage() . "\n" );

            return true;
        }

        if ( !$this->doDBUpdates() ) {
            return false;
        }

        if ( $db->insert( 'updatelog', [ 'ul_key' => $key ], __METHOD__, 'IGNORE' ) ) {
            return true;
        } else {
            $this->output( $this->updatelogFailedMessage() . "\n" );

            return false;
        }
    }

    protected function updateSkippedMessage() {
        $key = $this->getUpdateKey();

        return "Update '{$key}' already logged as completed.";
    }

    protected function updatelogFailedMessage() {
        $key = $this->getUpdateKey();

        return "Unable to log update '{$key}' as completed.";
    }

    abstract protected function doDBUpdates();

    abstract protected function getUpdateKey();
}