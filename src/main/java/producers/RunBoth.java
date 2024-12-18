package producers;

public class RunBoth {

    public static void main(String[] args) {
        try {
            Thread routesThread = new Thread(() -> {
                try {
                    RoutesProducerCenario.main(args);
                } catch (Exception e) {
                    System.err.println("Erro ao iniciar RoutesProducerCenario: " + e.getMessage());
                }
            });

            routesThread.start();

            System.out.println("Esperando o RoutesProducerCenario criar rotas...");
            Thread.sleep(5000);

            Thread tripsThread = new Thread(() -> {
                try {
                    TripsProducerCenario.main(args);
                } catch (Exception e) {
                    System.err.println("Erro ao iniciar TripsProducerCenario: " + e.getMessage());
                }
            });

            tripsThread.start();
            
            routesThread.join();
            tripsThread.join();
        } catch (Exception e) {
            System.err.println("Erro ao executar os produtores: " + e.getMessage());
        }
    }
}