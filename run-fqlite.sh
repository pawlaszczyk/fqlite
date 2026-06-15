#!/usr/bin/env sh
set -eu

JDK_HOME=""

if [ -n "${JAVA_HOME:-}" ] \
  && [ -x "$JAVA_HOME/bin/java" ] \
  && [ -f "$JAVA_HOME/release" ] \
  && grep -q '^JAVA_VERSION="21' "$JAVA_HOME/release"; then
  JDK_HOME="$JAVA_HOME"
elif [ -x "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home/bin/java" ]; then
  JDK_HOME="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
elif [ -x "/usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home/bin/java" ]; then
  JDK_HOME="/usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
elif [ -x "/Applications/Android Studio.app/Contents/jbr/Contents/Home/bin/java" ]; then
  JDK_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home"
fi

if [ -z "$JDK_HOME" ]; then
  printf '%s\n' "No compatible JDK 21 was found." >&2
  printf '%s\n' "Install it with: brew install openjdk@21" >&2
  printf '%s\n' "Then run: JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home ./gradlew run" >&2
  exit 1
fi

JAVA_HOME="$JDK_HOME" exec ./gradlew run
