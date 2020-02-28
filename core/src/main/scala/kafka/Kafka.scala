/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.server.{KafkaServer, KafkaServerStartable}
import kafka.utils.{CommandLineUtils, Logging}
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._

object Kafka extends Logging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    println(args(0))
    println(args(1))
    val optionParser = new OptionParser
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])

    val usernameOpt =  optionParser.accepts("username","REQUIRED: properties required to check user who has rights to do the action")
      .withRequiredArg
      .describedAs("username")
      .ofType(classOf[String])

    val passwordOpt =  optionParser.accepts("password","REQUIRED: properties required to check user who has rights to do the action")
      .withRequiredArg
      .describedAs("password")
      .ofType(classOf[String])
    if (args.length == 0) {
      CommandLineUtils.printUsageAndDie(optionParser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[KafkaServer].getSimpleName()))
    }

    val props = Utils.loadProps(args(0))

    if(args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      CommandLineUtils.checkRequiredArgs(optionParser,options, usernameOpt, passwordOpt)
      CommandLineUtils.checkUsernamePasswordArgs(optionParser,options.valueOf(usernameOpt),options.valueOf(passwordOpt))
      
      if(options.nonOptionArguments().size() > 0) {
         CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      props.putAll(CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt)))
    }else{
      System.err.println("USAGE: java [options] %s server.properties --username <username> --password <password> [--override property=value]*")
      optionParser.printHelpOn(System.err)
      sys.exit(1)
    }
    props
  }

  def main(args: Array[String]): Unit = {
    try {
      val serverProps = getPropsFromArgs(args)
      val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps)

      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run() = {
          kafkaServerStartable.shutdown
        }
      })

      kafkaServerStartable.startup
      kafkaServerStartable.awaitShutdown
    }
    catch {
      case e: Throwable =>
        fatal(e)
        System.exit(1)
    }
    System.exit(0)
  }


}
