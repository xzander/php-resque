<?php
use Psr\Log\LoggerInterface;
use Predis\Client;

/**
 * Wrap Predis to add namespace support and various helper methods.
 *
 * @package		Resque/Redis
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Resque_Redis
{
	/**
	 * Redis namespace
	 * @var string
	 */
	private static $defaultPrefix = 'resque:';

	/**
	 * A default host to connect to
	 */
	const DEFAULT_HOST = 'localhost';

	/**
	 * The default Redis port
	 */
	const DEFAULT_PORT = 6379;

	/**
	 * The default Redis Database number
	 */
	const DEFAULT_DATABASE = 0;

	private $driver;

	/**
	 *
	 * @var LoggerInterface
	 */
	private $logger;

	private $prefix;

	/**
	 * Set Redis namespace (prefix) default: resque
	 * @param string $namespace
	 */
	public static function prefix($namespace)
	{
	    if (substr($namespace, -1) !== ':') {
	        $namespace .= ':';
	    }
	    self::$defaultPrefix = $namespace;
	}

	/**
	 * @param string|array $server A DSN or array
	 * @param array $options Options for Predis client for replication or other features
	 * @param int $database A database number to select. However, if we find a valid database number in the DSN the
	 *                      DSN-supplied value will be used instead and this parameter is ignored.
	 */
    public function __construct($server, $options = [], $database = null)
	{
		if (!isset($options['prefix'])) {
			$options['prefix'] = self::$defaultPrefix;
		}

		$this->logger = false;

		if (is_array($server)) {
			$this->driver = new Client($server, $options);
		} else {
			$args = self::parseDsn($server);
			self::$redis = new Client($args, $options);

			// If we have found a database in our DSN, use it instead of the `$database`
			// value passed into the constructor.
			if ($args['database'] !== false) {
				$database = $args['database'];
			}
		}

		$this->prefix = $options['prefix'];

		if ($database !== null) {
			$this->driver->select($database);
		}
	}

	/**
	 *
	 * @param LoggerInterface $logger
	 */
	public function setLogger($logger) {
		$this->logger = $logger;
	}

	/**
	 * Parse a DSN string, which can have one of the following formats:
	 *
	 * - host:port
	 * - redis://user:pass@host:port/db?option1=val1&option2=val2
	 * - tcp://user:pass@host:port/db?option1=val1&option2=val2
	 *
	 * Note: the 'user' part of the DSN is not used.
	 *
	 * @param string $dsn A DSN string
	 * @return array An array of DSN compotnents, with 'false' values for any unknown components. e.g.
	 *               [host, port, db, user, pass, options]
	 */
	public static function parseDsn($dsn)
	{
		if ($dsn == '') {
			// Use a sensible default for an empty DNS string
			$dsn = 'redis://' . self::DEFAULT_HOST;
		}
		$parts = parse_url($dsn);

		// Check the URI scheme
		$validSchemes = array('redis', 'tcp');
		if (isset($parts['scheme']) && ! in_array($parts['scheme'], $validSchemes)) {
			throw new \InvalidArgumentException("Invalid DSN. Supported schemes are " . implode(', ', $validSchemes));
		}

		// Allow simple 'hostname' format, which `parse_url` treats as a path, not host.
		if ( ! isset($parts['host']) && isset($parts['path'])) {
			$parts['host'] = $parts['path'];
			unset($parts['path']);
		}

		// Extract the port number as an integer
		$port = isset($parts['port']) ? intval($parts['port']) : self::DEFAULT_PORT;

		// Get the database from the 'path' part of the URI
		$database = false;
		if (isset($parts['path'])) {
			// Strip non-digit chars from path
			$database = intval(preg_replace('/[^0-9]/', '', $parts['path']));
		}

		// Extract any 'user' and 'pass' values
		$user = isset($parts['user']) ? $parts['user'] : false;
		$pass = isset($parts['pass']) ? $parts['pass'] : false;

		// Convert the query string into an associative array
		$options = array();
		if (isset($parts['query'])) {
			// Parse the query string into an array
			parse_str($parts['query'], $options);
		}

		return array(
			"host" => $parts['host'],
			"port" => $port,
		);
	}

	/**
	 * Magic method to handle all function requests and prefix key based
	 * operations with the {self::$defaultNamespace} key prefix.
	 *
	 * @param string $name The name of the method called.
	 * @param array $args Array of supplied arguments to the method.
	 * @return mixed Return value from Resident::call() based on the command.
	 */
	public function __call($name, $args)
	{
		try {
			return $this->driver->__call($name, $args);
		}
		catch (\Predis\PredisException $e) {
			if ($this->logger) {
				$this->logger->critical("Could not call redis command [$name]: ".$e->getTraceAsString());
			}
			return false;
		}
	}

	public function getPrefix()
	{
		return $this->prefix;
	}
}
