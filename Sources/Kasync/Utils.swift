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

public func withCancellableCheckedThrowingContinuation<T>(
    function: String = #function,
    _ body: (CheckedContinuation<T, Error>, Cancellation) -> Void
) async throws -> T {
    let cancellationHolder = CancellationHolder()
    return try await withTaskCancellationHandler {
        return try await withCheckedThrowingContinuation(function: function) { (continuation: CheckedContinuation<T, Error>) -> Void in
            body(continuation, Cancellation(holder: cancellationHolder))
        }
    } onCancel: {
        cancellationHolder.onCancel()
    }
}

public func withCancellableCheckedContinuation<T>(
    function: String = #function,
    _ body: (CheckedContinuation<T, Never>, Cancellation) -> Void
) async -> T {
    let cancellationHolder = CancellationHolder()
    return await withTaskCancellationHandler {
        return await withCheckedContinuation(function: function) { (continuation: CheckedContinuation<T, Never>) -> Void in
            body(continuation, Cancellation(holder: cancellationHolder))
        }
    } onCancel: {
        cancellationHolder.onCancel()
    }
}

public struct Cancellation {
    
    fileprivate let holder: CancellationHolder
    
    public var onCancel: () -> Void {
        get {
            holder.onCancel
        }
        nonmutating set {
            holder.onCancel = newValue
        }
    }
    
}

fileprivate class CancellationHolder {
    var onCancel: (() -> Void) = {}
}

internal extension NSLocking {
    @discardableResult
    @inline(__always)
    func synchronized<T>(_ closure: () throws -> T) rethrows -> T {
        lock()
        defer { unlock() }
        return try closure()
    }
}

internal extension Array where Element: Equatable {
    
    @discardableResult
    mutating func remove(_ object: Element) -> Bool {
        if let index = firstIndex(of: object) {
            remove(at: index)
            return true
        }
        return false
    }
    
}

internal extension Array {
    
    mutating func popFirst() -> Element? {
        if isEmpty {
            return nil
        } else {
            return removeFirst()
        }
    }
    
    mutating func removeFirst(where predicate: (Element) -> Bool) -> Element? {
        guard let index = firstIndex(where: predicate) else { return nil }
        return remove(at: index)
    }
    
}
