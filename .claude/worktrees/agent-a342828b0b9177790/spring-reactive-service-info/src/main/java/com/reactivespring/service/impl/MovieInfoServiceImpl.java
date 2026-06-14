package com.reactivespring.service.impl;

import com.reactivespring.entity.MovieInfoDocument;
import com.reactivespring.repository.MovieInfoRepository;
import com.reactivespring.service.MovieInfoService;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class MovieInfoServiceImpl implements MovieInfoService {

    private final MovieInfoRepository movieInfoRepository;

    public MovieInfoServiceImpl(MovieInfoRepository movieInfoRepository) {
        this.movieInfoRepository = movieInfoRepository;
    }

    @Override
    public Mono<MovieInfoDocument> createMovieInfo(MovieInfoDocument movieInfoDocument) {
        return movieInfoRepository.save(movieInfoDocument)
                // .name("movie.info.db.query").metrics() tracks reactive stream metrics:
                // subscribe count, onNext count, and latency via Micrometer.
                .name("movie.info.db.query").metrics();
    }

    @Override
    public Flux<MovieInfoDocument> getAllMovies() {
        return movieInfoRepository.findAll()
                .name("movie.info.db.query").metrics();
    }

    @Override
    public Flux<MovieInfoDocument> getAllMoviesByYear(Integer year) {
        return movieInfoRepository.findByYear(year)
                .name("movie.info.db.query").metrics();
    }

    @Override
    public Flux<MovieInfoDocument> getAllMoviesByName(String name) {
        return movieInfoRepository.findByName(name)
                .name("movie.info.db.query").metrics();
    }

    @Override
    public Mono<MovieInfoDocument> getMovieById(String id) {
        return movieInfoRepository.findById(id)
                .name("movie.info.db.query").metrics();
    }

    @Override
    public Mono<MovieInfoDocument> upsertMovieInfo(MovieInfoDocument incoming, String id) {
        return movieInfoRepository.findById(id)
                .flatMap(existing -> {
                    var updated = new MovieInfoDocument(
                            existing.movieInfoId(),
                            incoming.name(),
                            incoming.year(),
                            incoming.cast(),
                            incoming.releaseDate());
                    return movieInfoRepository.save(updated);
                })
                .name("movie.info.db.query").metrics();
    }

    @Override
    public Mono<Void> deleteMovieInfo(String movieInfoId) {
        return movieInfoRepository.deleteById(movieInfoId);
    }

    @Override
    public Flux<MovieInfoDocument> saveAllReactive(Flux<MovieInfoDocument> documents) {
        return movieInfoRepository.saveAll(documents)
                .name("movie.info.db.query").metrics();
    }
}
