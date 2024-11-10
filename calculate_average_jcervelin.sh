#!/bin/sh
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


#jbang --javaagent=ap-loader@jvm-profiling-tools/ap-loader=start,event=cpu,file=profile.html -m io.jcervelin.CalculateAverage target/average-1.0.0-SNAPSHOT.jar /Users/julianodb/IdeaProjects/1brc/measurements_1B.txt true
JAVA_OPTS="-Xmx4G -Xms4G"
java $JAVA_OPTS -cp target/average-1.0.0-SNAPSHOT.jar io.jcervelin.CalculateAverage /Users/julianodb/IdeaProjects/1brc/measurements_1B.txt $1
