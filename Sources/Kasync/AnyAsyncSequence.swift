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

import Foundation

public struct AnyAsyncSequence<Element>: AsyncSequence {
    
    public typealias AsyncIterator = AnyAsyncIterator<Element>
    public typealias Element = Element

    private let _makeAsyncIterator: () -> AnyAsyncIterator<Element>

    public class AnyAsyncIterator<Element>: AsyncIteratorProtocol {
        
        public typealias Element = Element

        private let _next: () async throws -> Element?
        
        deinit {
            onClose?()
        }
        
        let onClose: (() -> Void)?

        init(next: @escaping () async throws -> Element?, onClose: (() -> Void)?) {
            self.onClose = onClose
            self._next = {
                try await next()
            }
        }

        public func next() async throws -> Element? {
            return try await _next()
        }
    }


    public init<S: AsyncSequence>(_ seq: S, onClose: (() -> Void)? = nil) where S.Element == Element {
        var itr = seq.makeAsyncIterator()
        _makeAsyncIterator = {
            AnyAsyncIterator<Element>(next: { try await itr.next() }, onClose: onClose)
        }
    }
    
    public init(next: @escaping () async throws -> Element?, onClose: (() -> Void)? = nil) {
        _makeAsyncIterator = {
            AnyAsyncIterator<Element>(next: next, onClose: onClose)
        }
    }

    public func makeAsyncIterator() -> AnyAsyncIterator<Element> {
        return _makeAsyncIterator()
    }

}

public extension AsyncSequence {
    
    func eraseToAnyAsyncSequence() -> AnyAsyncSequence<Element> {
        AnyAsyncSequence(self)
    }
    
}

public func iterate<Element>(_ elements: [Element]) -> AnyAsyncSequence<Element> {
    VectorAsyncSequence(elements: elements).eraseToAnyAsyncSequence()
}

public func iterate<Element>(count: Int, _ element: @autoclosure () -> Element) -> AnyAsyncSequence<Element> {
    ConstantAsyncSequence(count: count, element: element()).eraseToAnyAsyncSequence()
}

public func iterate(count: Int) -> AnyAsyncSequence<Void> {
    ConstantAsyncSequence(count: count, element: ()).eraseToAnyAsyncSequence()
}

public func iterate<Element>(_ element: @autoclosure () -> Element) -> AnyAsyncSequence<Element> {
    InfiniteAsyncSequence(element: element()).eraseToAnyAsyncSequence()
}

public func iterate() -> AnyAsyncSequence<Void> {
    InfiniteAsyncSequence(element: ()).eraseToAnyAsyncSequence()
}

private struct VectorAsyncSequence<Element> : AsyncSequence, AsyncIteratorProtocol {
    
    let elements: [Element]
   
    private var i: Int = 0
    
    public init(elements: [Element]) {
        self.elements = elements
    }

    mutating public func next() async throws -> Element? {
        if i >= elements.count {
            return nil
        } else {
            defer { i += 1 }
            return elements[i]
        }
    }

    public func makeAsyncIterator() -> VectorAsyncSequence {
        self
    }
    
}

private struct ConstantAsyncSequence<Element>: AsyncSequence, AsyncIteratorProtocol {
    
    let count: Int
    let element: Element
   
    private var i: Int = 0
    
    public init(count: Int, element: Element) {
        self.count = count
        self.element = element
    }

    mutating public func next() async throws -> Element? {
        if i >= count {
            return nil
        } else {
            i += 1
            return element
        }
    }

    public func makeAsyncIterator() -> ConstantAsyncSequence {
        self
    }
    
}

private struct InfiniteAsyncSequence<Element>: AsyncSequence, AsyncIteratorProtocol {
    
    let element: Element
    
    public init(element: Element) {
        self.element = element
    }

    mutating public func next() async throws -> Element? {
        try Task.checkCancellation()
        return element
    }

    public func makeAsyncIterator() -> InfiniteAsyncSequence {
        self
    }
    
}
