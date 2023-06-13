//
// DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
//
// Copyright (c) 2023 Andrey Kashaed. All rights reserved.
//
// The contents of this file are subject to the terms of the
// Common Development and Distribution License 1.0 (the "License").
// You may not use this file except in compliance with the License.
//
// You can obtain a copy of the License at
// https://opensource.org/licenses/CDDL-1.0 or LICENSE.txt.
//

@preconcurrency import Foundation

public func withCancellableCheckedThrowingContinuation<T>(
    function: String = #function,
    _ body: (CheckedContinuation<T, Error>, Cancellation) -> Void
) async throws -> T {
    let cancellation = Cancellation()
    return try await withTaskCancellationHandler {
        return try await withCheckedThrowingContinuation(function: function) { (continuation: CheckedContinuation<T, Error>) -> Void in
            body(continuation, cancellation)
        }
    } onCancel: {
        cancellation.onCancel()
    }
}

public func withCancellableCheckedContinuation<T>(
    function: String = #function,
    _ body: (CheckedContinuation<T, Never>, Cancellation) -> Void
) async -> T {
    let cancellation = Cancellation()
    return await withTaskCancellationHandler {
        return await withCheckedContinuation(function: function) { (continuation: CheckedContinuation<T, Never>) -> Void in
            body(continuation, cancellation)
        }
    } onCancel: {
        cancellation.onCancel()
    }
}

public struct Cancellation: Sendable {
    
    @UncheckedReference private var internalOnCancel: (() -> Void) = {}
    
    public var onCancel: () -> Void {
        get {
            internalOnCancel
        }
        nonmutating set {
            $internalOnCancel =^ newValue
        }
    }
    
}

@discardableResult
public func runBlocking<Success, Failure>(_ operation: @Sendable @escaping () async -> Result<Success, Failure>) -> Result<Success, Failure> {
    let semaphore = DispatchSemaphore(value: 0)
    @UncheckedReference var result: Result<Success, Failure>? = nil
    Task.detached {
        await $result =^ operation()
        semaphore.signal()
    }
    semaphore.wait()
    return result!
}

@discardableResult
public func runBlocking<Success>(_ operation: @Sendable @escaping () async throws -> Success) throws -> Success {
    let semaphore = DispatchSemaphore(value: 0)
    @UncheckedReference var result: Result<Success, Error>? = nil
    Task.detached {
        do {
            $result =^ .success(try await operation())
        } catch {
            $result =^ .failure(error)
        }
        semaphore.signal()
    }
    semaphore.wait()
    switch result! {
    case .success(let value):
        return value
    case .failure(let error):
        throw error
    }
}

@discardableResult
public func runBlocking<Success>(_ operation: @Sendable @escaping () async -> Success) -> Success {
    let semaphore = DispatchSemaphore(value: 0)
    @UncheckedReference var result: Success? = nil
    Task.detached {
        await $result =^ operation()
        semaphore.signal()
    }
    semaphore.wait()
    return result!
}

@discardableResult
public func runBlocking<Success>(timeout: DispatchTime, _ operation: @Sendable @escaping () async -> Success) -> Success? {
    let semaphore = DispatchSemaphore(value: 0)
    @UncheckedReference var result: Success? = nil
    Task.detached {
        await $result =^ operation()
        semaphore.signal()
    }
    switch semaphore.wait(timeout: timeout) {
    case .success:
        return result
    case .timedOut:
        return nil
    }
}

public extension AsyncStream {
    
    init(unfolding produce: @escaping () async -> Element?, onTerminate: @escaping () -> Void, onCancel: (@Sendable () -> Void)? = nil) {
        var terminated = false
        let produceWithTermination: () async -> Element? = {
            if terminated {
                return nil
            }
            let element = await produce()
            if element == nil {
                onTerminate()
                terminated = true
            }
            return element
        }
        self.init(unfolding: produceWithTermination, onCancel: onCancel)
    }
    
}

public extension AsyncThrowingStream {
    
    init(unfolding produce: @escaping () async throws -> Element?, onTerminate: @escaping () -> Void) where Failure == Error {
        var terminated = false
        let produceWithTermination: () async throws -> Element? = {
            if terminated {
                return nil
            }
            do {
                let element = try await produce()
                if element == nil {
                    onTerminate()
                    terminated = true
                }
                return element
            } catch {
                onTerminate()
                terminated = true
                throw error
            }
        }
        self.init(unfolding: produceWithTermination)
    }
    
}

public struct AsyncRethrowingStream<Element, I: AsyncIteratorProtocol>: AsyncSequence where I.Element == Element {
    
    public final class Iterator<Element, I: AsyncIteratorProtocol>: AsyncIteratorProtocol where I.Element == Element {
        
        private var iterator: I
        private let onTerminate: (() -> Void)?
        public private(set) var terminated = false
        
        fileprivate init(iterator: I, onTerminate: (() -> Void)?) {
            self.iterator = iterator
            self.onTerminate = onTerminate
        }
        
        deinit {
            if !terminated {
                onTerminate?()
            }
        }
        
        public func next() async rethrows -> Element? {
            if terminated {
                return nil
            }
            do {
                let element = try await iterator.next()
                if element == nil {
                    onTerminate?()
                    terminated = true
                }
                return element
            } catch {
                onTerminate?()
                terminated = true
                throw error
            }
        }
        
    }
    
    private let iterator: Iterator<Element, I>
    
    public init(iterator: I, onTerminate: (() -> Void)? = nil) {
        self.iterator = Iterator(iterator: iterator, onTerminate: onTerminate)
    }
    
    public var terminated: Bool {
        iterator.terminated
    }

    public func makeAsyncIterator() -> Iterator<Element, I> {
        iterator
    }
    
}

postfix operator *?

public postfix func *?<S: AsyncSequence>(_ sequence: S) -> AsyncStream<S.Element> {
    var iterator = sequence.makeAsyncIterator()
    return AsyncStream(unfolding: { try? await iterator.next() })
}

postfix operator *!

public postfix func *!<S: AsyncSequence>(_ sequence: S) -> AsyncThrowingStream<S.Element, Error> {
    var iterator = sequence.makeAsyncIterator()
    return AsyncThrowingStream(unfolding: { try await iterator.next() })
}

postfix operator *~

public postfix func *~<S: AsyncSequence>(_ sequence: S) -> AsyncRethrowingStream<S.Element, S.AsyncIterator> {
    let iterator = sequence.makeAsyncIterator()
    return AsyncRethrowingStream(iterator: iterator)
}

public func iterate<Element>(cancelable: Bool = true, elements: [Element]) -> AsyncStream<Element> {
    let count = elements.count
    var i = 0
    let next: () async -> Element? = {
        if i >= count || (cancelable && Task.isCancelled) {
            return nil
        }
        defer { i += 1 }
        return elements[i]
    }
    return AsyncStream(unfolding: next)
}

public func iterate<Element>(cancelable: Bool = true, elements: Element...) -> AsyncStream<Element> {
    let count = elements.count
    var i = 0
    let next: () async -> Element? = {
        if i >= count || (cancelable && Task.isCancelled) {
            return nil
        }
        defer { i += 1 }
        return elements[i]
    }
    return AsyncStream(unfolding: next)
}

public func iterate<Element>(cancelable: Bool = true, count: Int, element: @autoclosure @escaping () -> Element) -> AsyncStream<Element> {
    var i = 0
    let next: () async -> Element? = {
        if i >= count || (cancelable && Task.isCancelled) {
            return nil
        }
        defer { i += 1 }
        return element()
    }
    return AsyncStream(unfolding: next)
}

public func iterate(cancelable: Bool = true, count: Int) -> AsyncStream<Void> {
    iterate(cancelable: cancelable, count: count, element: ())
}

public func iterateInfinitely<Element>(element: @autoclosure @escaping () -> Element) -> AsyncStream<Element> {
    let next: () async -> Element? = {
        if Task.isCancelled {
            return nil
        }
        return element()
    }
    return AsyncStream(unfolding: next)
}

public func iterateInfinitely() -> AsyncStream<Void> {
    iterateInfinitely(element: ())
}
