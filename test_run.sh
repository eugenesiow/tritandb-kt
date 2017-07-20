for i in {1..100}
do
	./gradlew -PmainClass=com.tritandb.engine.query.engine.TestQueryKt -Pmyargs=1 execute >> test_overhead.txt 2>&1
done
