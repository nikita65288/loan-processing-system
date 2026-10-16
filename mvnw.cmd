@echo off
setlocal
set BASE_DIR=%~dp0
set WRAPPER_JAR=%BASE_DIR%.mvn\wrapper\maven-wrapper.jar
set WRAPPER_URL=https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.3.4/maven-wrapper-3.3.4.jar

if not exist "%WRAPPER_JAR%" (
  echo Downloading Maven Wrapper 3.3.4...
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -UseBasicParsing -Uri '%WRAPPER_URL%' -OutFile '%WRAPPER_JAR%'"
  if errorlevel 1 exit /b 1
)

java -classpath "%WRAPPER_JAR%" org.apache.maven.wrapper.MavenWrapperMain %*
endlocal
