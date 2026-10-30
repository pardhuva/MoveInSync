package com.moveinsync.livetrips.config;

import com.moveinsync.livetrips.model.TripMeta;
import com.moveinsync.livetrips.repository.TripRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Seeds a small, well-known world at startup so the service is immediately demonstrable and the
 * sample curl calls in the README work without any setup.
 *
 * <p>Two tenants (acme, globex) exist specifically so the tenant-isolation requirement can be
 * demonstrated live: an acme operator trying to watch a globex trip must be refused. Active only
 * when {@code livetrips.demo.seed-enabled=true}.
 */
@Component
@ConditionalOnProperty(name = "livetrips.demo.seed-enabled", havingValue = "true")
public class DemoDataSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoDataSeeder.class);

    private final TripRepository repository;

    public DemoDataSeeder(TripRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Tenant acme: office BLR-1 with two trips.
        repository.registerOffice("office-BLR-1", "acme");
        repository.save(new TripMeta("trip-100", "cab-7", "office-BLR-1", "acme", "emp-alice"));
        repository.save(new TripMeta("trip-101", "cab-8", "office-BLR-1", "acme", "emp-bob"));

        // Tenant globex: a separate office and trip, used to prove cross-tenant denial.
        repository.registerOffice("office-HYD-1", "globex");
        repository.save(new TripMeta("trip-200", "cab-99", "office-HYD-1", "globex", "emp-carol"));

        log.info("Seeded demo data: {} trips across tenants [acme, globex]. "
                + "Mint tokens at POST /api/v1/dev/token.", repository.tripCount());
    }
}
