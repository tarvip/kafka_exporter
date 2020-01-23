package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"hash"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	kazoo "github.com/krallistic/kazoo-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	plog "github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"github.com/xdg/scram"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "kafka_exporter"
	clientID  = "kafka_exporter"
)

var (
	clusterBrokers                     *prometheus.Desc
	topicPartitions                    *prometheus.Desc
	topicCurrentOffset                 *prometheus.Desc
	topicOldestOffset                  *prometheus.Desc
	topicPartitionLeader               *prometheus.Desc
	topicPartitionReplicas             *prometheus.Desc
	topicPartitionInSyncReplicas       *prometheus.Desc
	topicPartitionUsesPreferredReplica *prometheus.Desc
	topicUnderReplicatedPartition      *prometheus.Desc
	consumergroupCurrentOffset         *prometheus.Desc
	consumergroupCurrentOffsetSum      *prometheus.Desc
	consumergroupLag                   *prometheus.Desc
	consumergroupLagSum                *prometheus.Desc
	consumergroupLagZookeeper          *prometheus.Desc
	consumergroupMembers               *prometheus.Desc

	enabledMetrics             map[string]*prometheus.Desc
	enableTopicMetrics         bool
	enableConsumerGroupMetrics bool

	sha256HashGenFcn scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }
	sha512HashGenFcn scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

	opts kafkaOpts
)

type xdgSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *xdgSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *xdgSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

type topicPartitionConsumer struct {
	topic           string
	partition       int32
	consumerGroupID string
}

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client                  sarama.Client
	topicFilter             *regexp.Regexp
	topicExclude            *regexp.Regexp
	groupFilter             *regexp.Regexp
	groupExclude            *regexp.Regexp
	mu                      sync.Mutex
	useZooKeeperLag         bool
	zookeeperClient         *kazoo.Kazoo
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
}

type kafkaOpts struct {
	uri                      []string
	useSASL                  bool
	useSASLHandshake         bool
	saslUsername             string
	saslPassword             string
	saslMechanism            string
	useTLS                   bool
	tlsCAFile                string
	tlsCertFile              string
	tlsKeyFile               string
	tlsInsecureSkipTLSVerify bool
	kafkaVersion             string
	useZooKeeperLag          bool
	uriZookeeper             []string
	labels                   string
	metadataRefreshInterval  string
	enabledMetrics           []string
}

// CanReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if certReadable == false && keyReadable == false {
		return false, nil
	}

	if certReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if keyReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// If the file represented by path exists and
// readable, returns true otherwise returns false.
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}

// NewExporter returns an initialized Exporter.
func NewExporter(opts kafkaOpts, topicFilter string, topicExclude string, groupFilter string, groupExclude string) (*Exporter, error) {
	var zookeeperClient *kazoo.Kazoo
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.kafkaVersion)
	if err != nil {
		return nil, err
	}
	config.Version = kafkaVersion

	if opts.useSASL {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = opts.useSASLHandshake

		if opts.saslUsername != "" {
			config.Net.SASL.User = opts.saslUsername
		}

		if opts.saslPassword != "" {
			config.Net.SASL.Password = opts.saslPassword
		}

		if opts.saslMechanism != "" {

			if opts.saslMechanism == sarama.SASLTypeSCRAMSHA512 {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdgSCRAMClient{HashGeneratorFcn: sha512HashGenFcn} }
				config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
			} else if opts.saslMechanism == sarama.SASLTypeSCRAMSHA256 {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdgSCRAMClient{HashGeneratorFcn: sha256HashGenFcn} }
				config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
			} else {
				plog.Fatalf("Invalid SASL_MECHANISM \"%s\": can be either \"%s\" or \"%s\"\n", opts.saslMechanism, sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512)
			}
		}
	}

	if opts.useTLS {
		config.Net.TLS.Enable = true

		config.Net.TLS.Config = &tls.Config{
			RootCAs:            x509.NewCertPool(),
			InsecureSkipVerify: opts.tlsInsecureSkipTLSVerify,
		}

		if opts.tlsCAFile != "" {
			if ca, err := ioutil.ReadFile(opts.tlsCAFile); err == nil {
				config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				plog.Fatalln(err)
			}
		}

		canReadCertAndKey, err := CanReadCertAndKey(opts.tlsCertFile, opts.tlsKeyFile)
		if err != nil {
			plog.Fatalln(err)
		}
		if canReadCertAndKey {
			cert, err := tls.LoadX509KeyPair(opts.tlsCertFile, opts.tlsKeyFile)
			if err == nil {
				config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				plog.Fatalln(err)
			}
		}
	}

	if opts.useZooKeeperLag {
		zookeeperClient, err = kazoo.NewKazoo(opts.uriZookeeper, nil)
	}

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		plog.Errorln("Cannot parse metadata refresh interval")
		panic(err)
	}

	config.Metadata.RefreshFrequency = interval

	client, err := sarama.NewClient(opts.uri, config)

	if err != nil {
		plog.Errorln("Error Init Kafka Client")
		panic(err)
	}
	plog.Infoln("Done Init Clients")

	// Init our exporter.
	return &Exporter{
		client:                  client,
		topicFilter:             regexp.MustCompile(topicFilter),
		topicExclude:            regexp.MustCompile(topicExclude),
		groupFilter:             regexp.MustCompile(groupFilter),
		groupExclude:            regexp.MustCompile(groupExclude),
		useZooKeeperLag:         opts.useZooKeeperLag,
		zookeeperClient:         zookeeperClient,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
	}, nil
}

// Describe describes all the metrics ever exported by the Kafka exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {

	for _, v := range enabledMetrics {
		ch <- v
	}
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	var wg = sync.WaitGroup{}

	if val, ok := enabledMetrics["brokers"]; ok {
		ch <- prometheus.MustNewConstMetric(
			val, prometheus.GaugeValue, float64(len(e.client.Brokers())),
		)
	}

	if !enableTopicMetrics {
		return
	}

	offset := make(map[string]map[int32]int64)

	var consumerLagSecondsInput map[int32]map[topicPartitionConsumer]int64

	if metricsEnabled("lag_seconds") {
		consumerLagSecondsInput = make(map[int32]map[topicPartitionConsumer]int64)
	}

	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		plog.Info("Refreshing client metadata")

		if err := e.client.RefreshMetadata(); err != nil {
			plog.Errorf("Cannot refresh topics, using cached data: %v", err)
		}

		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	topics, err := e.client.Topics()
	if err != nil {
		plog.Errorf("Cannot get topics: %v", err)
		return
	}

	getTopicMetrics := func(topic string) {
		defer wg.Done()
		if e.topicFilter.MatchString(topic) && !e.topicExclude.MatchString(topic) {
			partitions, err := e.client.Partitions(topic)
			if err != nil {
				plog.Errorf("Cannot get partitions of topic %s: %v", topic, err)
				return
			}
			if val, ok := enabledMetrics["partitions"]; ok {
				ch <- prometheus.MustNewConstMetric(
					val, prometheus.GaugeValue, float64(len(partitions)), topic,
				)
			}
			e.mu.Lock()
			offset[topic] = make(map[int32]int64, len(partitions))
			e.mu.Unlock()
			for _, partition := range partitions {
				var broker *sarama.Broker
				var err error
				if val, ok := enabledMetrics["partition_leader"]; ok || metricsEnabled("partition_leader_is_preferred") {
					broker, err = e.client.Leader(topic, partition)
					if err != nil {
						plog.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
					} else {
						if val != nil {
							ch <- prometheus.MustNewConstMetric(
								val, prometheus.GaugeValue, float64(broker.ID()), topic, strconv.FormatInt(int64(partition), 10),
							)
						}
					}
				}

				var currentOffset int64
				if val, ok := enabledMetrics["partition_current_offset"]; ok || enableConsumerGroupMetrics || metricsEnabled("lag_zookeeper") {
					currentOffset, err = e.client.GetOffset(topic, partition, sarama.OffsetNewest)

					if err != nil {
						plog.Errorf("Cannot get current offset of topic %s partition %d: %v", topic, partition, err)
					} else {
						e.mu.Lock()
						offset[topic][partition] = currentOffset
						e.mu.Unlock()

						if val != nil {
							ch <- prometheus.MustNewConstMetric(
								val, prometheus.GaugeValue, float64(currentOffset), topic, strconv.FormatInt(int64(partition), 10),
							)
						}

					}
				}
				if val, ok := enabledMetrics["partition_oldest_offset"]; ok {
					oldestOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetOldest)
					if err != nil {
						plog.Errorf("Cannot get oldest offset of topic %s partition %d: %v", topic, partition, err)
					} else {
						ch <- prometheus.MustNewConstMetric(
							val, prometheus.GaugeValue, float64(oldestOffset), topic, strconv.FormatInt(int64(partition), 10),
						)
					}
				}

				var replicas []int32
				if val, ok := enabledMetrics["partition_replicas"]; ok || metricsEnabled("partition_leader_is_preferred", "partition_under_replicated_partition") {
					replicas, err = e.client.Replicas(topic, partition)
					if err != nil {
						plog.Errorf("Cannot get replicas of topic %s partition %d: %v", topic, partition, err)
					} else {

						if val != nil {
							ch <- prometheus.MustNewConstMetric(
								val, prometheus.GaugeValue, float64(len(replicas)), topic, strconv.FormatInt(int64(partition), 10),
							)
						}
					}
				}

				var inSyncReplicas []int32
				if val, ok := enabledMetrics["partition_in_sync_replica"]; ok || metricsEnabled("partition_under_replicated_partition") {
					inSyncReplicas, err = e.client.InSyncReplicas(topic, partition)
					if err != nil {
						plog.Errorf("Cannot get in-sync replicas of topic %s partition %d: %v", topic, partition, err)
					} else {
						if val != nil {
							ch <- prometheus.MustNewConstMetric(
								val, prometheus.GaugeValue, float64(len(inSyncReplicas)), topic, strconv.FormatInt(int64(partition), 10),
							)
						}
					}
				}
				if val, ok := enabledMetrics["partition_leader_is_preferred"]; ok {
					if broker != nil && replicas != nil && len(replicas) > 0 && broker.ID() == replicas[0] {
						ch <- prometheus.MustNewConstMetric(
							val, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
						)

					} else {
						ch <- prometheus.MustNewConstMetric(
							val, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
						)
					}
				}

				if val, ok := enabledMetrics["partition_under_replicated_partition"]; ok {
					if replicas != nil && inSyncReplicas != nil && len(inSyncReplicas) < len(replicas) {

						ch <- prometheus.MustNewConstMetric(
							val, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
						)
					} else {

						ch <- prometheus.MustNewConstMetric(
							val, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
						)
					}
				}

				if e.useZooKeeperLag {
					ConsumerGroups, err := e.zookeeperClient.Consumergroups()

					if err != nil {
						plog.Errorf("Cannot get consumer group %v", err)
					}

					for _, group := range ConsumerGroups {
						offset, _ := group.FetchOffset(topic, partition)
						if offset > 0 {

							if val, ok := enabledMetrics["lag_zookeeper"]; ok {
								consumerGroupLag := currentOffset - offset
								ch <- prometheus.MustNewConstMetric(
									val, prometheus.GaugeValue, float64(consumerGroupLag), group.Name, topic, strconv.FormatInt(int64(partition), 10),
								)
							}
						}
					}
				}
			}
		}
	}

	if enableTopicMetrics {
		for _, topic := range topics {
			wg.Add(1)
			go getTopicMetrics(topic)
		}

		wg.Wait()
	}

	getConsumerGroupMetrics := func(broker *sarama.Broker) {
		defer wg.Done()
		if err := broker.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			plog.Errorf("Cannot connect to broker %d: %v", broker.ID(), err)
			return
		}

		if !metricsEnabled("lag_seconds") {
			defer broker.Close()
		}

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			plog.Errorf("Cannot get consumer group: %v", err)
			return
		}
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			if e.groupFilter.MatchString(groupId) && !e.groupExclude.MatchString(groupId) {
				groupIds = append(groupIds, groupId)
			}
		}

		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			plog.Errorf("Cannot get describe groups: %v", err)
			return
		}
		for _, group := range describeGroups.Groups {
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
			for topic, partitions := range offset {
				for partition := range partitions {
					offsetFetchRequest.AddPartition(topic, partition)
				}
			}
			if val, ok := enabledMetrics["members"]; ok {
				ch <- prometheus.MustNewConstMetric(
					val, prometheus.GaugeValue, float64(len(group.Members)), group.GroupId,
				)
			}
			if offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest); err != nil {
				plog.Errorf("Cannot get offset of group %s: %v", group.GroupId, err)
			} else {
				for topic, partitions := range offsetFetchResponse.Blocks {
					// If the topic is not consumed by that consumer group, skip it
					topicConsumed := false
					for _, offsetFetchResponseBlock := range partitions {
						// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
						if offsetFetchResponseBlock.Offset != -1 {
							topicConsumed = true
							break
						}
					}
					if topicConsumed {
						var currentOffsetSum int64
						var lagSum int64
						for partition, offsetFetchResponseBlock := range partitions {
							err := offsetFetchResponseBlock.Err
							if err != sarama.ErrNoError {
								plog.Errorf("Error for  partition %d :%v", partition, err.Error())
								continue
							}
							currentOffset := offsetFetchResponseBlock.Offset

							if metricsEnabled("lag_seconds") {
								broker, err := e.client.Leader(topic, partition)
								if err != nil {
									plog.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
								} else {
									e.mu.Lock()
									consumerLagSecondsInput[broker.ID()][topicPartitionConsumer{topic: topic, partition: partition, consumerGroupID: group.GroupId}] = currentOffset
									e.mu.Unlock()
								}
							}

							currentOffsetSum += currentOffset
							if val, ok := enabledMetrics["current_offset"]; ok {
								ch <- prometheus.MustNewConstMetric(
									val, prometheus.GaugeValue, float64(currentOffset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
								)
							}
							e.mu.Lock()
							if offset, ok := offset[topic][partition]; ok {
								// If the topic is consumed by that consumer group, but no offset associated with the partition
								// forcing lag to -1 to be able to alert on that
								var lag int64
								if offsetFetchResponseBlock.Offset == -1 {
									lag = -1
								} else {
									lag = offset - offsetFetchResponseBlock.Offset
									lagSum += lag
								}
								if val, ok := enabledMetrics["lag"]; ok {
									ch <- prometheus.MustNewConstMetric(
										val, prometheus.GaugeValue, float64(lag), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
									)
								}
							} else {
								plog.Errorf("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
							}
							e.mu.Unlock()
						}
						if val, ok := enabledMetrics["current_offset_sum"]; ok {
							ch <- prometheus.MustNewConstMetric(
								val, prometheus.GaugeValue, float64(currentOffsetSum), group.GroupId, topic,
							)
						}
						if val, ok := enabledMetrics["lag_sum"]; ok {
							ch <- prometheus.MustNewConstMetric(
								val, prometheus.GaugeValue, float64(lagSum), group.GroupId, topic,
							)
						}
					}
				}
			}
		}
	}

	getConsumerGroupLagSeconds := func(broker *sarama.Broker, consumerLagSecondsInput map[topicPartitionConsumer]int64) {
		defer wg.Done()
		defer broker.Close()

		if promDesc, ok := enabledMetrics["lag_seconds"]; ok {
			for tpo, offset := range consumerLagSecondsInput {
				fetchRequest := &sarama.FetchRequest{MaxWaitTime: 500, Version: 4}
				fetchRequest.AddBlock(tpo.topic, tpo.partition, offset, 1) // maxBytes is also set on serverside anyway

				response, err := broker.Fetch(fetchRequest)
				if err != nil {
					plog.Errorf("Failed to fetch messages %s, %d, %d: %v", tpo.topic, tpo.partition, offset, err)
				} else {
					currentTimestamp := time.Now()
					offsetTimestamp := currentTimestamp

					for _, partitions := range response.Blocks {
						for _, respBlock := range partitions {
							for _, r := range respBlock.RecordsSet {
								if r.RecordBatch != nil {
									if r.RecordBatch.FirstOffset != offset {
										plog.Warnf("Offset mismatch %s, %d: %d (request) <-> %d (response) ", tpo.topic, tpo.partition, offset, r.RecordBatch.FirstOffset)
									}
									offsetTimestamp = r.RecordBatch.FirstTimestamp
								}
							}
						}
					}

					ch <- prometheus.MustNewConstMetric(
						promDesc, prometheus.GaugeValue, float64(currentTimestamp.Sub(offsetTimestamp).Seconds()), tpo.consumerGroupID, tpo.topic, strconv.FormatInt(int64(tpo.partition), 10),
					)

				}
			}
		}
	}

	if len(e.client.Brokers()) > 0 {
		if enableConsumerGroupMetrics {
			for _, broker := range e.client.Brokers() {
				if metricsEnabled("lag_seconds") {
					consumerLagSecondsInput[broker.ID()] = make(map[topicPartitionConsumer]int64)
				}

				wg.Add(1)
				go getConsumerGroupMetrics(broker)
			}
			wg.Wait()

			if metricsEnabled("lag_seconds") {
				for _, broker := range e.client.Brokers() {

					if val, ok := consumerLagSecondsInput[broker.ID()]; ok {
						go getConsumerGroupLagSeconds(broker, val)
					}

					wg.Add(1)
				}
				wg.Wait()
			}
		}

	} else {
		plog.Errorln("No valid broker, cannot get consumer group metrics")
	}
}

func metricsEnabled(metricNames ...string) bool {
	for _, m := range metricNames {
		if _, ok := enabledMetrics[m]; ok {
			return true
		}
	}

	return false
}

func contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func addEnabledMetric(subsystem, name, help string, variableLabels []string, constLabels prometheus.Labels) {
	if len(opts.enabledMetrics) == 1 && opts.enabledMetrics[0] == "" || contains(opts.enabledMetrics, name) {

		if subsystem == "topic" {
			enableTopicMetrics = true
		}

		if subsystem == "consumergroup" {
			enableTopicMetrics = true
			enableConsumerGroupMetrics = true
		}

		p := prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, name),
			help,
			variableLabels, constLabels,
		)

		enabledMetrics[name] = p
		return
	}
}

func init() {
	metrics.UseNilMetrics = true
	prometheus.MustRegister(version.NewCollector("kafka_exporter"))

	opts = kafkaOpts{}
	enabledMetrics = make(map[string]*prometheus.Desc)
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9308").Envar("WEB_LISTEN_ADDRESS").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").Envar("WEB_TELEMETRY_PATH").String()
		topicFilter   = kingpin.Flag("topic.filter", "Regex that determines which topics to collect.").Default(".*").Envar("TOPIC_FILTER").String()
		topicExclude  = kingpin.Flag("topic.exclude", "Regex that determines which topics to exclude.").Default("^ ").Envar("TOPIC_EXCLUDE").String()
		groupFilter   = kingpin.Flag("group.filter", "Regex that determines which consumer groups to collect.").Default(".*").Envar("GROUP_FILTER").String()
		groupExclude  = kingpin.Flag("group.exclude", "Regex that determines which consumer groups to exclude.").Default("^ ").Envar("GROUP_EXCLUDE").String()
		logSarama     = kingpin.Flag("log.enable-sarama", "Turn on Sarama logging.").Default("false").Envar("LOG_ENABLE_SARAMA").Bool()
	)

	kingpin.Flag("kafka.server", "Address (host:port) of Kafka server.").Default("kafka:9092").Envar("KAFKA_SERVER").StringsVar(&opts.uri)
	kingpin.Flag("sasl.enabled", "Connect using SASL/PLAIN.").Default("false").Envar("SASL_ENABLED").BoolVar(&opts.useSASL)
	kingpin.Flag("sasl.handshake", "Only set this to false if using a non-Kafka SASL proxy.").Default("true").Envar("SASL_HANDSHAKE").BoolVar(&opts.useSASLHandshake)
	kingpin.Flag("sasl.username", "SASL user name.").Default("").Envar("SASL_USERNAME").StringVar(&opts.saslUsername)
	kingpin.Flag("sasl.password", "SASL user password.").Default("").Envar("SASL_PASSWORD").StringVar(&opts.saslPassword)
	kingpin.Flag("sasl.mechanism", "SASL mechanism.").Default("").Envar("SASL_MECHANISM").StringVar(&opts.saslMechanism)
	kingpin.Flag("tls.enabled", "Connect using TLS.").Default("false").Envar("TLS_ENABLED").BoolVar(&opts.useTLS)
	kingpin.Flag("tls.ca-file", "The optional certificate authority file for TLS client authentication.").Default("").Envar("TLS_CA_FILE").StringVar(&opts.tlsCAFile)
	kingpin.Flag("tls.cert-file", "The optional certificate file for client authentication.").Default("").Envar("TLS_CERT_FILE").StringVar(&opts.tlsCertFile)
	kingpin.Flag("tls.key-file", "The optional key file for client authentication.").Default("").Envar("TLS_KEY_FILE").StringVar(&opts.tlsKeyFile)
	kingpin.Flag("tls.insecure-skip-tls-verify", "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure.").Default("false").Envar("TLS_INSECURE_SKIP_TLS_VERIFY").BoolVar(&opts.tlsInsecureSkipTLSVerify)
	kingpin.Flag("kafka.version", "Kafka broker version").Default(sarama.V1_0_0_0.String()).Envar("KAFKA_VERSION").StringVar(&opts.kafkaVersion)
	kingpin.Flag("use.consumelag.zookeeper", "if you need to use a group from zookeeper").Default("false").Envar("USE_CONSUMELAG_ZOOKEEPER").BoolVar(&opts.useZooKeeperLag)
	kingpin.Flag("zookeeper.server", "Address (hosts) of zookeeper server.").Default("localhost:2181").Envar("ZOOKEEPER_SERVER").StringsVar(&opts.uriZookeeper)
	kingpin.Flag("kafka.labels", "Kafka cluster name").Default("").Envar("KAFKA_LABELS").StringVar(&opts.labels)
	kingpin.Flag("refresh.metadata", "Metadata refresh interval").Default("30s").Envar("REFRESH_METADATA").StringVar(&opts.metadataRefreshInterval)
	kingpin.Flag("enabled.metric", "Enabled metrics, by default all will be collected").Default("").Envar("ENABLED_METRIC").StringsVar(&opts.enabledMetrics)

	plog.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	plog.Infoln("Starting kafka_exporter", version.Info())
	plog.Infoln("Build context", version.BuildContext())

	labels := make(map[string]string)

	// Protect against empty labels
	if opts.labels != "" {
		for _, label := range strings.Split(opts.labels, ",") {
			splitted := strings.Split(label, "=")
			if len(splitted) >= 2 {
				labels[splitted[0]] = splitted[1]
			}
		}
	}

	addEnabledMetric("", "brokers",
		"Number of Brokers in the Kafka Cluster.",
		nil, labels)

	addEnabledMetric("topic", "partitions",
		"Number of partitions for this Topic",
		[]string{"topic"}, labels)

	addEnabledMetric("topic", "partition_current_offset",
		"Current Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, labels)

	addEnabledMetric("topic", "partition_oldest_offset",
		"Oldest Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, labels)

	addEnabledMetric("topic", "partition_leader",
		"Leader Broker ID of this Topic/Partition",
		[]string{"topic", "partition"}, labels)

	addEnabledMetric("topic", "partition_replicas",
		"Number of Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, labels)

	addEnabledMetric("topic", "partition_in_sync_replica",
		"Number of In-Sync Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, labels)

	addEnabledMetric("topic", "partition_leader_is_preferred",
		"1 if Topic/Partition is using the Preferred Broker",
		[]string{"topic", "partition"}, labels)

	addEnabledMetric("topic", "partition_under_replicated_partition",
		"1 if Topic/Partition is under Replicated",
		[]string{"topic", "partition"}, labels)

	addEnabledMetric("consumergroup", "current_offset",
		"Current Offset of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	addEnabledMetric("consumergroup", "current_offset_sum",
		"Current Offset of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	addEnabledMetric("consumergroup", "lag",
		"Current Approximate Lag of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	addEnabledMetric("consumergroupzookeeper", "lag_zookeeper",
		"Current Approximate Lag(zookeeper) of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)

	addEnabledMetric("consumergroup", "lag_sum",
		"Current Approximate Lag of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	addEnabledMetric("consumergroup", "members",
		"Amount of members in a consumer group",
		[]string{"consumergroup"}, labels,
	)

	addEnabledMetric("consumergroup", "lag_seconds",
		"Current Approximate Lag of a ConsumerGroup at Topic/Partition in seconds",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	if *logSarama {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	exporter, err := NewExporter(opts, *topicFilter, *topicExclude, *groupFilter, *groupExclude)
	if err != nil {
		plog.Fatalln(err)
	}
	defer exporter.client.Close()
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
	        <head><title>Kafka Exporter</title></head>
	        <body>
	        <h1>Kafka Exporter</h1>
	        <p><a href='` + *metricsPath + `'>Metrics</a></p>
	        </body>
	        </html>`))
	})

	plog.Infoln("Listening on", *listenAddress)
	plog.Fatal(http.ListenAndServe(*listenAddress, nil))
}
