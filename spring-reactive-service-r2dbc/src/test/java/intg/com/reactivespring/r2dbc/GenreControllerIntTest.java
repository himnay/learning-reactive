package intg.com.reactivespring.r2dbc;

import com.reactivespring.r2dbc.R2dbcServiceApplication;
import com.reactivespring.r2dbc.entity.Genre;
import com.reactivespring.r2dbc.repository.GenreRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT, classes = R2dbcServiceApplication.class)
@Testcontainers
class GenreControllerIntTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    WebTestClient webTestClient;

    @Autowired
    GenreRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll()
                .thenMany(repository.saveAll(List.of(
                        Genre.of("Action", "Fast-paced movies"),
                        Genre.of("Drama", "Character-driven stories"),
                        Genre.of("Comedy", "Humorous films")
                )))
                .blockLast();
    }

    @Test
    @DisplayName("GET /v1/genres returns all genres")
    void findAll() {
        webTestClient.get().uri("/v1/genres")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Genre.class)
                .hasSize(3);
    }

    @Test
    @DisplayName("POST /v1/genres creates a genre within a transaction")
    void create() {
        Genre newGenre = Genre.of("Thriller", "Suspenseful movies");

        webTestClient.post().uri("/v1/genres")
                .bodyValue(newGenre)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(Genre.class)
                .value(g -> {
                    assertThat(g.id()).isNotNull();
                    assertThat(g.name()).isEqualTo("Thriller");
                });
    }

    @Test
    @DisplayName("POST /v1/genres/batch inserts multiple genres in one transaction")
    void createBatch() {
        List<Genre> batch = List.of(
                Genre.of("Horror", "Scary movies"),
                Genre.of("Romance", "Love stories"),
                Genre.of("Sci-Fi", "Science fiction")
        );

        webTestClient.post().uri("/v1/genres/batch")
                .bodyValue(batch)
                .exchange()
                .expectStatus().isCreated()
                .expectBodyList(Genre.class)
                .hasSize(3)
                .value(list -> assertThat(list).allMatch(g -> g.id() != null));
    }

    @Test
    @DisplayName("PUT /v1/genres/{id} updates genre name")
    void update() {
        Genre existing = repository.findByName("Action").block();
        assertThat(existing).isNotNull();

        Genre updated = Genre.of("Action Updated", "Updated description");

        webTestClient.put().uri("/v1/genres/{id}", existing.id())
                .bodyValue(updated)
                .exchange()
                .expectStatus().isOk()
                .expectBody(Genre.class)
                .value(g -> assertThat(g.name()).isEqualTo("Action Updated"));
    }

    @Test
    @DisplayName("DELETE /v1/genres/{id} removes genre")
    void delete() {
        Genre existing = repository.findByName("Drama").block();
        assertThat(existing).isNotNull();

        webTestClient.delete().uri("/v1/genres/{id}", existing.id())
                .exchange()
                .expectStatus().isNoContent();

        StepVerifier.create(repository.findById(existing.id()))
                .verifyComplete();
    }

    @Test
    @DisplayName("GET /v1/genres/search filters by name substring")
    void search() {
        webTestClient.get().uri("/v1/genres/search?name=edy")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Genre.class)
                .hasSize(1)
                .value(list -> assertThat(list.get(0).name()).isEqualTo("Comedy"));
    }

    @Test
    @DisplayName("POST /v1/genres returns 400 for blank name")
    void createValidation() {
        Genre invalid = Genre.of("", "no name");

        webTestClient.post().uri("/v1/genres")
                .bodyValue(invalid)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("Duplicate batch insert rolls back entire transaction")
    void batchRollbackOnDuplicate() {
        // "Action" already exists; this batch should fail and roll back "NewOne"
        List<Genre> batch = List.of(
                Genre.of("NewOne", "will be rolled back"),
                Genre.of("Action", "duplicate — triggers constraint violation")
        );

        webTestClient.post().uri("/v1/genres/batch")
                .bodyValue(batch)
                .exchange()
                .expectStatus().is5xxServerError();

        // NewOne must NOT be in the DB (transaction rolled back)
        StepVerifier.create(repository.findByName("NewOne"))
                .verifyComplete();
    }
}
