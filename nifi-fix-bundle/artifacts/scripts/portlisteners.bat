start "matchingout" java -jar nifiporttostdout.jar http://localhost:8080/nifi matchingout
start "orderbookout" java -jar nifiporttostdout.jar http://localhost:8080/nifi orderbookout
start "orderslog" java -jar nifiporttostdout.jar http://localhost:8080/nifi fixorderslogging
start "orderslog" java -jar nifiporttostdout.jar http://localhost:8080/nifi weborderslogging