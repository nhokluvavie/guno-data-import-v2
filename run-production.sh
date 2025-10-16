#!/bin/bash

# Production startup script for Guno Data Import - Facebook Platform
echo "🚀 Starting Guno Data Import - Facebook Platform (Production)"

# Load environment variables
if [ -f .env.production ]; then
    export $(cat .env.production | grep -v '^#' | xargs)
    echo "✅ Environment variables loaded"
else
    echo "⚠️ .env.production not found, using default values"
fi

# Create logs directory
mkdir -p logs
echo "✅ Logs directory created"

# Check if application jar exists
JAR_FILE=$(find target -name "guno-data-import-*.jar" | head -1)
if [ -z "$JAR_FILE" ]; then
    echo "❌ Application jar not found. Please run: mvn clean package"
    exit 1
fi
echo "✅ Found application jar: $JAR_FILE"

# Set default JVM options if not provided
if [ -z "$JAVA_OPTS" ]; then
    JAVA_OPTS="-Xms1g -Xmx2g -XX:+UseG1GC"
fi

# Production JVM options
JAVA_OPTS="$JAVA_OPTS -Dspring.profiles.active=prod"
JAVA_OPTS="$JAVA_OPTS -Dlogging.config=classpath:logback-spring.xml"
JAVA_OPTS="$JAVA_OPTS -Djava.security.egd=file:/dev/./urandom"

echo "✅ JVM Options: $JAVA_OPTS"

# Health check function
health_check() {
    local max_attempts=30
    local attempt=1

    echo "🔍 Waiting for application to start..."

    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:${SERVER_PORT:-8080}/actuator/health > /dev/null 2>&1; then
            echo "✅ Application is healthy!"
            return 0
        fi
        echo "   Attempt $attempt/$max_attempts - waiting..."
        sleep 5
        ((attempt++))
    done

    echo "❌ Application failed to start within timeout"
    return 1
}

# Start application
echo "🚀 Starting application..."
echo "📅 $(date): Starting Guno Data Import Facebook Platform" >> logs/startup.log

nohup java $JAVA_OPTS -jar $JAR_FILE > logs/application.out 2>&1 &
APP_PID=$!

echo "✅ Application started with PID: $APP_PID"
echo $APP_PID > logs/app.pid

# Wait for startup and health check
sleep 10
if health_check; then
    echo "🎉 Production deployment successful!"
    echo "📊 Health endpoint: http://localhost:${SERVER_PORT:-8080}/actuator/health"
    echo "📈 Metrics endpoint: http://localhost:${SERVER_PORT:-8080}/actuator/metrics"
    echo "📋 Logs: tail -f logs/guno-data-import.log"
    echo "🔄 Scheduler will run every 2 hours automatically"
else
    echo "❌ Production deployment failed - check logs"
    exit 1
fi

---

# ===============================================
# File: stop-production.sh (stop script)
# ===============================================
#!/bin/bash

echo "🛑 Stopping Guno Data Import - Facebook Platform"

if [ -f logs/app.pid ]; then
    PID=$(cat logs/app.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "🔄 Stopping application (PID: $PID)..."
        kill $PID

        # Wait for graceful shutdown
        sleep 10

        if ps -p $PID > /dev/null 2>&1; then
            echo "⚠️ Force killing application..."
            kill -9 $PID
        fi

        rm logs/app.pid
        echo "✅ Application stopped"
    else
        echo "⚠️ Application not running"
        rm logs/app.pid
    fi
else
    echo "⚠️ PID file not found"
fi

echo "📅 $(date): Stopped Guno Data Import Facebook Platform" >> logs/startup.log