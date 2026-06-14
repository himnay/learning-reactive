package com.reactivespring.service.impl;

import com.reactivespring.entity.MovieInfoDocument;
import com.reactivespring.repository.MovieInfoRepository;
import com.reactivespring.service.MovieInfoService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class MovieInfoServiceImpl implements MovieInfoService {

    private final MovieInfoRepository movieInfoRepository;

    @Override
    public Mono<MovieInfoDocument> createMovieInfo(MovieInfoDocument movieInfoDocument) {
        return movieInfoRepository.save(movieInfoDocument);
    }

    @Override
    public Flux<MovieInfoDocument> getAllMovies() {
        return movieInfoRepository.findAll();
    }

    @Override
    public Flux<MovieInfoDocument> getAllMoviesByYear(Integer year) {
        return movieInfoRepository.findByYear(year);
    }

    @Override
    public Flux<MovieInfoDocument> getAllMoviesByName(String name) {
        return movieInfoRepository.findByName(name);
    }

    @Override
    public Mono<MovieInfoDocument> getMovieById(String id) {
        return movieInfoRepository.findById(id);
    }

    @Override
    public Mono<MovieInfoDocument> upsertMovieInfo(MovieInfoDocument movieInfoDocument, String id) {
        return movieInfoRepository.findById(id)
                .flatMap(i -> {
                    i.setName(movieInfoDocument.getName());
                    i.setMovieInfoId(movieInfoDocument.getMovieInfoId());
                    i.setCast(movieInfoDocument.getCast());
                    i.setYear(movieInfoDocument.getYear());
                    i.setReleaseDate(movieInfoDocument.getReleaseDate());
                    return movieInfoRepository.save(i);
                });
    }

    @Override
    public Mono<Void> deleteMovieInfo(String movieInfoId) {
        return movieInfoRepository.deleteById(movieInfoId);
    }
}
