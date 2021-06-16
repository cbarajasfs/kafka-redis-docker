package com.finsol.Sink;

import com.finsol.Redis.RedisConnectorConfig;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class MySinkConnectorConfig extends RedisConnectorConfig {

  public final static String OPERATION_TIMEOUT_MS_CONF = "redis.operation.timeout.ms";
  final static String OPERATION_TIMEOUT_MS_DOC = "The amount of time in milliseconds before an" +
          " operation is marked as timed out.";

  public final static String CHARSET_CONF = "redis.charset";
  public final static String CHARSET_DOC = "The character set to use for String key and values.";

  public final long operationTimeoutMs;
  public final Charset charset;

  public MySinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.operationTimeoutMs = getLong(OPERATION_TIMEOUT_MS_CONF);
    String charset = getString(CHARSET_CONF);
    this.charset = Charset.forName(charset);
  }
  public static ConfigDef config() {
    return RedisConnectorConfig.config()
            .define(
                    ConfigKeyBuilder.of(OPERATION_TIMEOUT_MS_CONF, ConfigDef.Type.LONG)
                            .documentation(OPERATION_TIMEOUT_MS_DOC)
                            .defaultValue(10000L)
                            .validator(ConfigDef.Range.atLeast(100L))
                            .importance(ConfigDef.Importance.MEDIUM)
                            .build()
            ).define(
                    ConfigKeyBuilder.of(CHARSET_CONF, ConfigDef.Type.STRING)
                            .documentation(CHARSET_DOC)
                            .defaultValue("UTF-8")
                            .validator(Validators.validCharset())
                            .recommender(Recommenders.charset())
                            .importance(ConfigDef.Importance.LOW)
                            .build()
            );
  }
}
